#pragma once

#if defined(__linux__)
#include <linux/openat2.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
#else
struct open_how;
struct statx;
#endif
