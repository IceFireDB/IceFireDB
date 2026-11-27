//go:build !darwin

package guts

import "github.com/klauspost/cpuid/v2"

var (
	haveSSE41  = cpuid.CPU.Supports(cpuid.SSE4, cpuid.SSSE3)
	haveAVX2   = cpuid.CPU.Supports(cpuid.AVX2)
	haveAVX512 = cpuid.CPU.Supports(cpuid.AVX512F)
)
