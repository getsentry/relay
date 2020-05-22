import random

from infrastructure.generators.util import schema_generator, string_databag_generator

BASE_IMAGES_WITH_FRAMES = [
    (
        {
            "type": "apple",
            "arch": "x86_64",
            "uuid": "502fc0a5-1ec1-3e47-9998-684fa139dca7",
            "image_vmaddr": "0x0000000100000000",
            "image_size": 4096,
            "image_addr": "0x0000000100000000",
            "name": "Foo.app/Contents/Foo",
        },
        [{"instruction_addr": "0x0000000100000fa0"}]
    )
]

def native_data_generator(**event_kwargs):
    """
    Generate an event with one native stacktrace + debug_images section. The
    debug images to successfully symbolicate the generated crash are in
    `tests/load-tests/native-images/`
    """

    def inner():
        frames = []
        images = []

        for image, image_frames in BASE_IMAGES_WITH_FRAMES:
            image = dict(image)
            shift = int(random.random() * 2000) - 1000

            image['image_addr'] = hex(int(image['image_addr'], 16) + shift)
            images.append(image)

            for frame in image_frames:
                frame = dict(frame)
                frame['instruction_addr'] = hex(int(frame['instruction_addr'], 16) + shift)
                frames.append(frame)

        return frames, images

    return inner
