# Synthetic integration videos

FFmpeg was not installed during implementation, so no MP4 fixtures were generated.
No software was installed. When FFmpeg with `libx264` is available, run the following
from the repository root on macOS, Linux, Windows CMD, or PowerShell (one command per line):

```text
ffmpeg -version
ffmpeg -f lavfi -i testsrc2=size=320x240:rate=30 -t 3 -c:v libx264 -pix_fmt yuv420p -movflags +faststart test-videos/small-3s.mp4
ffmpeg -f lavfi -i testsrc2=size=640x360:rate=30 -t 10 -c:v libx264 -pix_fmt yuv420p -movflags +faststart test-videos/medium-10s.mp4
ffmpeg -f lavfi -i testsrc2=size=1280x720:rate=30 -t 25 -c:v libx264 -pix_fmt yuv420p -movflags +faststart test-videos/large-25s.mp4
```

These are synthetic, silent H.264/yuv420p videos with different durations,
resolutions and expected sizes. Existing files are not overwritten without a prompt.
MP4 files are already ignored by Git. Optionally verify each with:

```text
ffprobe -v error -show_entries stream=codec_name,pix_fmt,width,height -show_entries format=duration,size -of json test-videos/small-3s.mp4
```

After Person 1 and Person 3 finish, compare source and uploaded file sizes and hashes:

macOS:
```sh
shasum -a 256 test-videos/small-3s.mp4 uploads/small-3s.mp4
```

Windows PowerShell:
```powershell
Get-FileHash test-videos/small-3s.mp4, uploads/small-3s.mp4 -Algorithm SHA256
```

Also exercise simultaneous uploads, equal filenames with different contents,
interrupted streams and (after queue integration) overflow. Validate that accepted
files are complete and incomplete/rejected staging files are removed.
