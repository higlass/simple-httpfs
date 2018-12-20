DIR=x11
umount /tmp/$DIR/https
python simple-httpfs.py /tmp/$DIR/https https

cat /tmp/${DIR}/https/s3.amazonaws.com/pkerp/public/tiny.txt..

umount $DIR/https
python simple-httpfs.py $DIR/https https

cat $DIR/https/s3.amazonaws.com/pkerp/public/tiny.txt..

umount /tmp/$DIR/https
umount $DIR/https
