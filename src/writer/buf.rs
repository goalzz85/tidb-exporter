use std::io::Write;


pub struct LinkedBuffer {
    block_size : usize,
    blocks : Vec<Box<Vec<u8>>>,
    capacity : usize,
    used_size : usize,
}

#[allow(unused_variables, dead_code)]
impl LinkedBuffer {
    pub fn new(block_size : usize, block_num : usize) -> LinkedBuffer {
        return LinkedBuffer {
            block_size,
            blocks : Vec::with_capacity(block_num),
            capacity : block_size * block_num,
            used_size : 0,
        }
    }

    pub fn block_size(&self) -> usize {
        return self.block_size;
    }

    pub fn len(&self) -> usize {
        return self.used_size;
    }

    pub fn reset(&mut self) {
        self.used_size = 0;
    }

    pub fn write_to <T : std::io::Write> (&self, w : &mut T) -> std::io::Result<usize> {
        let mut writed_size : usize = 0;
        for block in &self.blocks {
            let mut block_need_write_size = self.used_size - writed_size;
            if block_need_write_size == 0 {
                break;
            }

            if block_need_write_size > self.block_size {
                block_need_write_size = self.block_size;
            }

            let mut block_writed_size : usize = 0;
            loop {
                if let Ok(s) = w.write(&block[block_writed_size..block_need_write_size]) {
                    block_writed_size += s;
                    if block_need_write_size == block_writed_size {
                        writed_size += block_need_write_size;
                        break;
                    }
                }
            }
        }

        Ok(writed_size)
    }

    pub fn get_cur_writable_block<'a>(&'a mut self) -> std::io::Result<&'a mut Vec<u8>> {
        if self.used_size == self.capacity {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "out of the buffer's capacity"));
        }

        let block_idx = self.used_size / self.block_size;

        if self.blocks.len() > block_idx {
            if let Some(block_mut_ref) = self.blocks.get_mut(block_idx) {
                return Ok(block_mut_ref);
            } else {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "get a writable block failed"));
            }
        } else {
            self.blocks.push(Box::new(vec![0; self.block_size]));
            if let Some(block_mut_ref) = self.blocks.get_mut(block_idx) {
                return Ok(block_mut_ref);
            } else {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "allocating new block failed"));
            }
        }
    }
}

impl Write for LinkedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let buf_original_len = buf.len();
        let mut buf_mut_ref = buf;
        if buf_original_len + self.used_size > self.capacity {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "out of the buffer's capacity"));
        }
         
        loop {
            let write_start_idx = self.used_size % self.block_size;
            let block_free_size = self.block_size - write_start_idx;

            let block = self.get_cur_writable_block()?;
            let copy_size = if buf_mut_ref.len() > block_free_size { block_free_size } else { buf_mut_ref.len() };
            let writable_block = &mut block[write_start_idx..write_start_idx + copy_size];
            let readable_buf = &buf_mut_ref[..copy_size];
            writable_block.copy_from_slice(readable_buf);
            
            self.used_size += copy_size;
            if copy_size == buf_mut_ref.len() {
                break;
            }
            buf_mut_ref = &buf_mut_ref[copy_size..];
        }

        Ok(buf_original_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}