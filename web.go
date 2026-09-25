package main

func runWeb(cfg *Config, listen string) {
	if listen == "" {
		throwFmt("web: -listen is required")
	}

	throwFmt("web: not implemented")
}
