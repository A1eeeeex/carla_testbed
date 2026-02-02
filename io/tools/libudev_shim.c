#define _GNU_SOURCE
#include <dlfcn.h>
#include <libudev.h>
#include <stddef.h>

typedef const char *(*udev_get_action_fn)(struct udev_device *);

static udev_get_action_fn load_real() {
    static udev_get_action_fn fn = NULL;
    if (fn) return fn;
    void *h = dlopen("libudev.so.1", RTLD_LAZY | RTLD_LOCAL);
    if (!h) return NULL;
    fn = (udev_get_action_fn)dlsym(h, "udev_device_get_action");
    return fn;
}

const char *udev_device_get_action(struct udev_device *dev) {
    udev_get_action_fn fn = load_real();
    return fn ? fn(dev) : NULL;
}

const char *_udev_device_get_action(struct udev_device *dev) __attribute__((alias("udev_device_get_action")));
