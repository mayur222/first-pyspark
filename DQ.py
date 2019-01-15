from pyspark import SparkContext;
from pyspark.sql import Row,SQLContext
import sys
import itertools
path = sys.argv[1]
sc = SparkContext()
sqlc = SQLContext(sc);
df = sqlc.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load(path)
s=path.split('.');
x=str("");
for a in s[:len(s)-1]:
	x+=a

x+='-stats.csv'
descibed = df.describe()
#descibed.show()
descibed.toPandas().to_csv(x)
print "saved in ",x
cols = df.columns
count = df.distinct().count()
print "Total records ",df.count()
uni=[];
for c in cols:
	x=df.groupBy(c).count().count();
	if x == count:
		print c,"is a unique column"

cf=False
for L in range(1,len(cols)+1):
	for comb in itertools.combinations(cols,L):
		x1=df.select(list(comb)).distinct().count();
		if count==x1:
			uni=list(comb)
			if len(uni)>1:
				print comb,"is unique combination"
			cf=True
			break
	if cf :
		break

x=df.count()

if x==count:
	print "No duplicates rows"
else:
	print x-count,"rows are duplicates"
	dx=df.groupBy(cols).count();
	print "Duplicates::"
	dx.filter(dx["count"]>1).show();
