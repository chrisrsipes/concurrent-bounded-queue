for i in {1..100}
do
  echo "run $i"
  time ./a.out $i 100
done
