package main

func runFront(cfg *Config, listen string) {
	if listen == "" {
		throwFmt("front: -listen is required")
	}

	throwFmt("front: not implemented")
}
