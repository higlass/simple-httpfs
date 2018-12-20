umount /tmp/x10
python simple-httpfs.py /tmp/x10

cat /tmp/x10/https/s3.amazonaws.com/pkerp/public/tiny.txt..

umount test-dir10/
python simple-httpfs.py test-dir10/

cat test-dir10/https/s3.amazonaws.com/pkerp/public/tiny.txt..
