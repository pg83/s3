package main

func runCell(listen, ssd, hdd string) {
	if listen == "" || ssd == "" || hdd == "" {
		throwFmt("cell: -listen, -ssd and -hdd are required")
	}

	throwFmt("cell: not implemented")
}
