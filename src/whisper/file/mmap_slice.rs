use mmap::Mmap;

use std::cell::{ RefCell, Ref };
use std::rc::Rc;
use std::ops::Deref;

struct MmapSlice {
	mmap: Rc<RefCell<Mmap>>,
	begin: usize,
	end: Option<usize>
}

impl MmapSlice {
	pub fn new(mmap_data: Rc<RefCell<Mmap>>, begin: usize, end: Option<usize>) -> MmapSlice {
		MmapSlice {
			mmap: mmap_data,
			begin: begin,
			end: end
		}
	}

	pub fn slice_ref(&self) -> Ref<&[u8]> {
		match self.end {
			Some(e) => &self.mmap.borrow()[ self.begin .. e ],
			None =>    &self.mmap.borrow()[ self.begin ..   ]
		}
	}

	pub fn slice_ref_mut(&self) -> Ref<&mut [u8]> {
		match self.end {
			Some(e) => &self.mmap.borrow_mut()[ self.begin .. e ],
			None    => &self.mmap.borrow_mut()[ self.begin ..   ]
		}
	}

	pub fn len(&self) -> usize {
		match self.end {
			Some(e) => e - self.begin,
			None => {
				let raw_len = self.mmap.borrow().len();
				raw_len - self.begin
			}
		}
	}
}
