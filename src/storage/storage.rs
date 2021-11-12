// FileType represent a file type.
// type FileType int

// // File types.
// const (
// 	TypeManifest FileType = 1 << iota
// 	TypeJournal
// 	TypeTable
// 	TypeTemp

// 	TypeAll = TypeManifest | TypeJournal | TypeTable | TypeTemp
// )
#[derive(Debug, Clone, Copy)]
pub enum FileType {
    TypeManifest = 1,
    TypeJournal = 2,
    TypeTable = 4,
    TypeTemp = 8,
    TypeAll = 1 | 2 | 4 | 8,
}
// func (t FileType) String() string {
// 	switch t {
// 	case TypeManifest:
// 		return "manifest"
// 	case TypeJournal:
// 		return "journal"
// 	case TypeTable:
// 		return "table"
// 	case TypeTemp:
// 		return "temp"
// 	}
// 	return fmt.Sprintf("<unknown:%d>", t)
// }

// FileDesc is a 'file descriptor'.
// type FileDesc struct {
// 	Type FileType
// 	Num  int64
// }
#[derive(Debug, Clone, Copy)]
pub struct FileDesc {
    pub file_type: FileType,
    pub num: i64,
}
