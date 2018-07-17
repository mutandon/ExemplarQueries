#!/bin/bash
set -eu -o pipefail
IFS=$'\n\t'

usage() {
	echo "Usage: $0 [-o OUT_DIR]  [-t TAG]" 1>&2; exit 1;
}

OUTDIR="./ExQ"
TAG=""

while getopts "ho:t:" opt; do
  case ${opt} in
    o)
      OUTDIR=${OPTARG}
      [[ -d $OUTDIR ]] || mkdir $OUTDIR  || usage
      ;;
    t)
      TAG="-${OPTARG}"
      [[ -n $TAG ]] || usage
      ;;
   \?)
      echo "Invalid option: -$OPTARG" >&2
      usage
      ;;
   :)
      echo "Option -$OPTARG requires an argument." >&2
      usage
      ;;
    h|*)
      usage
      ;;

  esac
done

([[ -d ExemplarQueries ]]  || (echo "Error in installation location" && exit 2))

HERE=`pwd`
([[ -d MUtilities ]] ||  git clone https://github.com/mutandon/MUtilities.git MUtilities) && cd MUtilities && git pull && mvn install && cd $HERE
([[ -d Grava ]] || git clone https://github.com/mutandon/Grava.git  Grava)  && cd Grava && git pull &&  mvn install && cd  $HERE
([[ -d ExecutionUtilities ]] || git clone https://github.com/mutandon/ExecutionUtilities.git ExecutionUtilities) && cd ExecutionUtilities && git pull && mvn install && cd  $HERE
([[ -d ExemplarQueries ]] && cd ExemplarQueries  && mvn install && cd  $HERE) || (echo "Error in installation location" && exit 2)

echo "Deploy to $OUTDIR"
mkdir -pv $OUTDIR/lib${TAG}
mkdir -pv $OUTDIR/scripts
JAR='ExQ.jar'
mv -v ./ExemplarQueries/target/${JAR} ${OUTDIR}/"${JAR%.jar}${TAG}.jar"
mv -v ./ExemplarQueries/target/lib/* $OUTDIR/lib${TAG}/
cp -v ./ExemplarQueries/scripts/prepare.sh $OUTDIR/scripts
cp -v ./ExemplarQueries/scripts/run.sh $OUTDIR/scripts
echo "Done!"
echo "If this is the first time Now Enter ${OUTDIR} and run ./scripts/prepare.sh"
echo "Otherwise test ./scripts/run.sh"





