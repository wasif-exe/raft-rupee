use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::alloc::{alloc, dealloc, Layout};
use raft_core::types::{LogIndex, Term, NodeId};
use raft_core::message::LogEntry;

pub const SECTOR_SIZE: usize = 4096;

pub struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
    pub size: usize,
}

impl AlignedBuffer {
    pub fn new(size: usize) -> Self {
        let rounded_size = (size + SECTOR_SIZE - 1) & !(SECTOR_SIZE - 1);
        let layout = Layout::from_size_align(rounded_size, SECTOR_SIZE).unwrap();
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            panic!("Aligned allocation failed");
        }
        Self { ptr, layout, size: rounded_size }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr, self.layout) };
    }
}

pub struct DurableDisk {
    base_dir: PathBuf,
    hs_file: File,
    wal_file: File,
}

impl DurableDisk {
    pub fn open<P: AsRef<Path>>(base_dir: P) -> std::io::Result<Self> {
        let base = base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&base)?;

        let hs_path = base.join("hard_state.dat");
        let wal_path = base.join("wal.log");

        let hs_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_DIRECT | libc::O_DSYNC)
            .open(hs_path)?;

        let wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_DIRECT | libc::O_DSYNC)
            .open(wal_path)?;

        Ok(Self {
            base_dir: base,
            hs_file,
            wal_file,
        })
    }

    pub fn write_hard_state(&mut self, term: Term, voted_for: Option<NodeId>) -> std::io::Result<()> {
        let mut buffer = AlignedBuffer::new(SECTOR_SIZE);
        
        let term_bytes = term.0.to_le_bytes();
        let voted_val = voted_for.map(|n| n.0).unwrap_or(0);
        let voted_bytes = voted_val.to_le_bytes();

        buffer.as_mut_slice()[0..8].copy_from_slice(&term_bytes);
        buffer.as_mut_slice()[8..16].copy_from_slice(&voted_bytes);

        let checksum = crc32fast::hash(&buffer.as_slice()[0..16]);
        buffer.as_mut_slice()[16..20].copy_from_slice(&checksum.to_le_bytes());

        unsafe {
            let fd = self.hs_file.as_raw_fd();
            let res = libc::pwrite(
                fd,
                buffer.ptr as *const libc::c_void,
                SECTOR_SIZE,
                0,
            );
            if res == -1 {
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(())
    }

    pub fn read_hard_state(&mut self) -> std::io::Result<Option<(Term, Option<NodeId>)>> {
        let buffer = AlignedBuffer::new(SECTOR_SIZE);

        unsafe {
            let fd = self.hs_file.as_raw_fd();
            let res = libc::pread(
                fd,
                buffer.ptr as *mut libc::c_void,
                SECTOR_SIZE,
                0,
            );
            if res == -1 {
                return Err(std::io::Error::last_os_error());
            }
            if res == 0 {
                return Ok(None);
            }
        }

        let slice = buffer.as_slice();
        let mut term_bytes = [0u8; 8];
        let mut voted_bytes = [0u8; 8];
        let mut crc_bytes = [0u8; 4];

        term_bytes.copy_from_slice(&slice[0..8]);
        voted_bytes.copy_from_slice(&slice[8..16]);
        crc_bytes.copy_from_slice(&slice[16..20]);

        let term = u64::from_le_bytes(term_bytes);
        let voted_val = u64::from_le_bytes(voted_bytes);
        let expected_crc = u32::from_le_bytes(crc_bytes);

        let actual_crc = crc32fast::hash(&slice[0..16]);
        if actual_crc != expected_crc && expected_crc != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Hard state CRC corruption detected!",
            ));
        }

        let voted_for = if voted_val == 0 { None } else { Some(NodeId(voted_val)) };
        Ok(Some((Term(term), voted_for)))
    }

    pub fn append_log_entries(&mut self, entries: &[LogEntry]) -> std::io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut payload = Vec::new();
        for entry in entries {
            payload.extend_from_slice(&entry.index.0.to_le_bytes());
            payload.extend_from_slice(&entry.term.0.to_le_bytes());
            payload.extend_from_slice(&(entry.command.len() as u32).to_le_bytes());
            payload.extend_from_slice(&entry.command);
            
            let crc = crc32fast::hash(&entry.command);
            payload.extend_from_slice(&crc.to_le_bytes());
        }

        let mut buffer = AlignedBuffer::new(payload.len());
        buffer.as_mut_slice()[..payload.len()].copy_from_slice(&payload);

        unsafe {
            let fd = self.wal_file.as_raw_fd();
            let end_offset = libc::lseek(fd, 0, libc::SEEK_END);
            if end_offset == -1 {
                return Err(std::io::Error::last_os_error());
            }

            let res = libc::pwrite(
                fd,
                buffer.ptr as *const libc::c_void,
                buffer.size,
                end_offset,
            );
            if res == -1 {
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(())
    }

    pub fn truncate_and_append(&mut self, from_index: LogIndex, entries: &[LogEntry]) -> std::io::Result<()> {
        let mut preserved = self.replay_wal()?;
        if let Some(pos) = preserved.iter().position(|e| e.index >= from_index) {
            preserved.truncate(pos);
        }

        unsafe {
            let fd = self.wal_file.as_raw_fd();
            if libc::ftruncate(fd, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::lseek(fd, 0, libc::SEEK_SET) == -1 {
                return Err(std::io::Error::last_os_error());
            }
        }

        self.append_log_entries(&preserved)?;
        self.append_log_entries(entries)?;
        Ok(())
    }

    pub fn replay_wal(&mut self) -> std::io::Result<Vec<LogEntry>> {
        let mut file = File::open(self.base_dir.join("wal.log"))?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let mut entries = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            if offset + 24 > data.len() {
                break;
            }

            let mut idx_bytes = [0u8; 8];
            let mut term_bytes = [0u8; 8];
            let mut len_bytes = [0u8; 4];

            idx_bytes.copy_from_slice(&data[offset..offset+8]);
            term_bytes.copy_from_slice(&data[offset+8..offset+16]);
            len_bytes.copy_from_slice(&data[offset+16..offset+20]);

            let index = u64::from_le_bytes(idx_bytes);
            let term = u64::from_le_bytes(term_bytes);
            let len = u32::from_le_bytes(len_bytes) as usize;

            if offset + 24 + len > data.len() {
                break;
            }

            let mut command = vec![0u8; len];
            command.copy_from_slice(&data[offset+20..offset+20+len]);

            let mut crc_bytes = [0u8; 4];
            crc_bytes.copy_from_slice(&data[offset+20+len..offset+24+len]);
            let expected_crc = u32::from_le_bytes(crc_bytes);

            let actual_crc = crc32fast::hash(&command);
            if actual_crc != expected_crc {
                break;
            }

            entries.push(LogEntry {
                index: LogIndex(index),
                term: Term(term),
                command,
            });

            offset += 24 + len;
        }

        Ok(entries)
    }
}
