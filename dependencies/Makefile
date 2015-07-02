build: clone
	cd lms && git checkout topic-ml && git pull && sbt publishLocal 
	cd scalan-lite-public && git checkout fixes-for-SVD-211 && git pull && sbt publishLocal 
	#"project scalan-core" publishLocal "project scalan-common" publishLocal "project scalan-libraty" publishLocal "project scalan-meta" publishLocal "project scalan-lms-backend" publishLocal
	cd scalan-ml && git checkout master-for-fixes-forSVD && git pull && sbt publishLocal 
	#cd scalan-spark && git checkout topic-ml && git pull && sbt test 

rebuild: clean clone
	cd lms && git checkout topic-ml && git pull && sbt clean update publishLocal 
	cd scalan-lite-public && git checkout fixes-for-SVD-211 && git pull  && sbt clean update publishLocal 
	cd scalan-ml && git checkout master-for-fixes-forSVD && git pull  && sbt clean update publishLocal 
	#cd scalan-spark && git checkout topic-ml && git pull && sbt clean update test 

#clone: lms scalan-lite-public scalan-ml scalan-spark
clone: lms scalan-lite-public scalan-ml

lms:
	git clone git@10.122.85.33:scalan-lite/lms.git

scalan-lite-public:
	git clone git@10.122.85.33:scalan-lite/scalan-lite-public.git

scalan-ml:
	git clone git@10.122.85.33:vsmirnov/scalan-ml.git

scalan-spark:
	git clone git@10.122.85.33:mgekk/scalan-spark.git

clean:
	rm ~/.ivy2/cache/EPFL/ -r || true
	rm ~/.ivy2/cache/com.huawei.scalan/ -r  || true
	rm ~/.ivy2/local/EPFL/ -r  || true
	rm ~/.ivy2/local/com.huawei.scalan/ -r  || true

	git checkout factories_on_companions

