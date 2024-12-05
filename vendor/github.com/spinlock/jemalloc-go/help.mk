.DEFAULT_GOAL = relink

PWD := $(shell pwd)
SRC := jemalloc-5.2.1

-include $(SRC)/Makefile

relink: unlink
	@for i in $(C_SRCS); do \
		rm -f            je_$$(basename $$i); \
		ln -s $(SRC)/$$i je_$$(basename $$i); \
	done

unlink:
	@rm -f je_*.c
