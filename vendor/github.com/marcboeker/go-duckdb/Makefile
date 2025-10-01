DUCKDB_REPO=https://github.com/duckdb/duckdb.git
DUCKDB_BRANCH=v1.1.3

.PHONY: install
install:
	go install .

.PHONY: examples
examples:
	go run examples/appender/main.go
	go run examples/json/main.go
	go run examples/scalar_udf/main.go
	go run examples/simple/main.go
	go run examples/table_udf/main.go
	go run examples/table_udf_parallel/main.go

.PHONY: test
test:
	go test -v -race -count=1 .

.PHONY: deps.header
deps.header:
	git clone -b ${DUCKDB_BRANCH} --depth 1 ${DUCKDB_REPO}
	cp duckdb/src/include/duckdb.h duckdb.h

.PHONY: duckdb
duckdb:
	rm -rf duckdb
	git clone -b ${DUCKDB_BRANCH} --depth 1 ${DUCKDB_REPO}

DUCKDB_COMMON_BUILD_FLAGS := BUILD_SHELL=0 BUILD_UNITTESTS=0 DUCKDB_PLATFORM=any ENABLE_EXTENSION_AUTOLOADING=1 ENABLE_EXTENSION_AUTOINSTALL=1 BUILD_EXTENSIONS="json"

.PHONY: deps.darwin.amd64
deps.darwin.amd64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "darwin" ]; then echo "Error: must run build on darwin"; false; fi
	mkdir -p deps/darwin_amd64

	cd duckdb && \
	CFLAGS="-target x86_64-apple-macos11 -O3" CXXFLAGS="-target x86_64-apple-macos11 -O3" ${DUCKDB_COMMON_BUILD_FLAGS} make bundle-library -j 2
	cp duckdb/build/release/libduckdb_bundle.a deps/darwin_amd64/libduckdb.a

.PHONY: deps.darwin.arm64
deps.darwin.arm64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "darwin" ]; then echo "Error: must run build on darwin"; false; fi
	mkdir -p deps/darwin_arm64

	cd duckdb && \
	CFLAGS="-target arm64-apple-macos11 -O3" CXXFLAGS="-target arm64-apple-macos11 -O3" ${DUCKDB_COMMON_BUILD_FLAGS}  make bundle-library -j 2
	cp duckdb/build/release/libduckdb_bundle.a deps/darwin_arm64/libduckdb.a

.PHONY: deps.linux.amd64
deps.linux.amd64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "linux" ]; then echo "Error: must run build on linux"; false; fi
	mkdir -p deps/linux_amd64

	cd duckdb && \
	CFLAGS="-O3" CXXFLAGS="-O3" ${DUCKDB_COMMON_BUILD_FLAGS} make bundle-library -j 2
	cp duckdb/build/release/libduckdb_bundle.a deps/linux_amd64/libduckdb.a

.PHONY: deps.linux.arm64
deps.linux.arm64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "linux" ]; then echo "Error: must run build on linux"; false; fi
	mkdir -p deps/linux_arm64

	cd duckdb && \
	CC="aarch64-linux-gnu-gcc" CXX="aarch64-linux-gnu-g++" CFLAGS="-O3" CXXFLAGS="-O3" ${DUCKDB_COMMON_BUILD_FLAGS} make bundle-library -j 2
	cp duckdb/build/release/libduckdb_bundle.a deps/linux_arm64/libduckdb.a

.PHONY: deps.freebsd.amd64
deps.freebsd.amd64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "freebsd" ]; then echo "Error: must run build on freebsd"; false; fi
	mkdir -p deps/freebsd_amd64

	cd duckdb && \
	CFLAGS="-O3" CXXFLAGS="-O3" ${DUCKDB_COMMON_BUILD_FLAGS} gmake bundle-library -j 2
	cp duckdb/build/release/libduckdb_bundle.a deps/freebsd_amd64/libduckdb.a

.PHONY: deps.windows.amd64
deps.windows.amd64: duckdb
	if [ "$(shell uname -s | tr '[:upper:]' '[:lower:]')" != "mingw64_nt-10.0-20348" ]; then echo "Error: must run build on windows"; false; fi
	mkdir -p deps/windows_amd64

	# Copied from the DuckDB repository and fixed for Windows. Ideally, `make bundle-library` should also work for Windows.
	cd duckdb && \
	${DUCKDB_COMMON_BUILD_FLAGS} GENERATOR="-G \"MinGW Makefiles\"" gmake release -j 2
	cd duckdb/build/release && \
		mkdir -p bundle && \
		cp src/libduckdb_static.a bundle/. && \
		cp third_party/*/libduckdb_*.a bundle/. && \
		cp extension/*/lib*_extension.a bundle/.
	cd duckdb/build/release/bundle && \
		find . -name '*.a' -exec ${AR} -x {} \;
	cd duckdb/build/release/bundle && \
		${AR} cr ../libduckdb_bundle.a *.obj

	cp duckdb/build/release/libduckdb_bundle.a deps/windows_amd64/libduckdb.a
