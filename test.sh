
# This is the directory where the filesystem will be mounted
DIR=media

# Create the folder if it doesn't exist
if [ ! -d "$DIR" ]; then
    mkdir "$DIR"
fi

if [ ! -d "$DIR/https" ]; then
    mkdir "$DIR/https"
fi

# Unmount and mount the filesystem
umount $DIR/https
simple-httpfs $DIR/https  --schema https

# This is a lorem ipsum text. It is used to test the filesystem
cat $DIR/https/s3.amazonaws.com/pkerp/public/tiny.txt.. 

# Unmount the filesystem since we are done
umount $DIR/https
