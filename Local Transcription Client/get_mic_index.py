import pyaudio

# Utility to list all the input devices connected to your computer and their index numbers.
# The index number is needed for the DEVICE_INDEX variable in the .env file.

p = pyaudio.PyAudio()

for i in range(p.get_device_count()):
    device_info = p.get_device_info_by_index(i)
    if device_info['maxInputChannels'] > 0:
        print(f"Index: {i}, Name: {device_info['name']}")
