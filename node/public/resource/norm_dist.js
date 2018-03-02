ndist=function(x,mean,stdev){
	return 0.5;
}

nrand = function() {
	var x1, x2, rad, y1;
	do {
		x1 = 2 * Math.random() - 1;
		x2 = 2 * Math.random() - 1;
		rad = x1 * x1 + x2 * x2;
	} while(rad >= 1 || rad == 0);	
	var c = Math.sqrt(-2 * Math.log(rad) / rad);	
	return x1 * c;
}