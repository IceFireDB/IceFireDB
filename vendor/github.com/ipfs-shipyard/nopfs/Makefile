plugin:
	$(MAKE) -C nopfs-kubo-plugin all

install-plugin:
	$(MAKE) -C nopfs-kubo-plugin install

dist-plugin:
	$(MAKE) -C nopfs-kubo-plugin dist

check:
	go vet ./...
	staticcheck --checks all ./...
	misspell -error -locale US .

.PHONY: plugin install-plugin check
