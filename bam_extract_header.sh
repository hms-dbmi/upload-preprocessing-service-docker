#!/usr/bin/env bash

samtools view -H $1 > /scratch/header.sam
rm -Rf /scratch/changelog.txt
sed -i -e "s/$2/$3/gw /scratch/changelog.txt" /scratch/header.sam