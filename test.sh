DIR=x11
umount /tmp/$DIR/https
python simple_httpfs/httpfs.py /tmp/$DIR/https

cat /tmp/${DIR}/https/s3.amazonaws.com/pkerp/public/tiny.txt..

umount $DIR/https
python simple_httpfs/httpfs.py $DIR/https

cat $DIR/https/s3.amazonaws.com/pkerp/public/tiny.txt..

umount /tmp/$DIR/https
umount $DIR/https

python simple_httpfs/httpfs.py $DIR/http

head $DIR/http/hgdownload.cse.ucsc.edu/goldenpath/hg19/encodeDCC/wgEncodeSydhTfbs/wgEncodeSydhTfbsGm12878InputStdSig.bigWig..

umount $DIR/http
