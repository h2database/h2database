#!/bin/sh

rm -Rf h2mig
java -cp bin org.h2.build.upgrade.UpgradeCreator h2 h2mig
cd h2mig
/bin/sh ./build.sh jar
cd ..
cp h2mig/bin/h2-1.2.128.jar h2mig_pagestore_addon.jar
rm -Rf h2mig
