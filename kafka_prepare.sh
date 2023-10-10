set -ex

#poetry run python create_topic.py

sum=10000000
per_thread=1000000

i=0
for ((j=1; j<=$((sum/per_thread)); j++))
do
    nohup poetry run python generator.py -b $i -e $((i+$per_thread)) &
    i=$((i+$per_thread))
done
