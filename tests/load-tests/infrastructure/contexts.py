import random

def schema_generator(**fields):
    """
    Generate a dictionary instance according to the schema outlined by the
    provided kwargs. Supports:

    * string
    * number
    * callable
    * range object
    * list of any of the above (random item will be selected)
    """
    def inner():
        rv = {}
        for k, sub_generator in fields.items():
            if isinstance(sub_generator, (list, tuple, range)):
                sub_generator = random.choice(sub_generator)

            if callable(sub_generator):
                v = sub_generator()

            if v is not None:
                rv[k] = v

        return rv

    return inner


def version_generator(n=3):
    def inner():
        return ".".join(str(random.randrange(10)) for _ in range(n))

    return inner


def device_context_generator():
    return schema_generator(
        type="device",
        screen_resolution=[None, lambda: f"{int(random.random() * 1000)}x{int(random.random() * 1000)}"],
        orientation=["portrait", "landscape", "garbage data", None],
        name=[None, lambda: f"Android SDK built for x{random.random()}"],
        family=[None, lambda: f"Device family {random.random()}"],
        battery_level=range(101),
        screen_dpi=range(1000),
        memory_size=range(10 ** 6),
        timezone=["America/Los_Angeles", "Europe/Vienna"],
        external_storage_size=range(10 ** 6),
        external_free_storage=range(10 ** 6),
        screen_width_pixels=range(1000),
        low_memory=[True, False],
        simulator=[True, False],
        screen_height_pixels=range(1000),
        free_memory=range(10 ** 5),
        online=[True, False],
        screen_density=range(5),
        charging=[True, False],
        locale=["DE", "US", "NL", "ES", "CZ"],
        model_id="NYC",
        brand=["google", "zoogle", "moodle", "doodle", "tamagotchi"],
        storage_size=range(10 ** 6),
        boot_time=time.time(),
        arch=lambda: f"x{random.random()}",
        manufacturer=["Google", "Hasbro"],
    )

def app_context_generator():
    return schema_generator(
        type="app",
        app_version=version_generator(3)
        app_identifier="io.sentry.sample",
        app_build=range(100),
    )

def os_context_generator():
    return schema_generator(
        type="os",
        rooted=[True, False],
        kernel_version="Linux version 3.10.0+ (bjoernj@bjoernj.mtv.corp.google.com) (gcc version 4.9.x 20150123 (prerelease) (GCC) ) #256 SMP PREEMPT Fri May 19 11:58:12 PDT 2017",
        version=version_generator(3),
        build="sdk_google_phone_x86-userdebug 7.1.1 NYC 5464897 test-keys",
        name=["Android", "NookPhone"]
    )

def mobile_contexts_generator():
    device_context = device_context_generator()
    app_context = app_context_generator()
    os_context =  os_context_generator()

    def inner():
        return {
            "app": app_context(),
            "os": os_context(),
            "device": device_context()
        }

    return inner
