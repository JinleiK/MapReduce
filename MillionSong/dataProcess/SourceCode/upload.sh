ROOT=$(pwd)

for i in {0..9998}
do	
	s3cmd put $i.h5 s3://jinleik/H5Converter/data1/$i.h5
done