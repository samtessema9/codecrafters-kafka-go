package main 

func checkVersion(version uint16) uint16 {
	if version >= 0 && version <= 4 {
		return uint16(0)
	} 
	return uint16(35)
}