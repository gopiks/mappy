
var app = angular.module("optionPricer", []); 
app.controller("myCtrl", function($scope) {

	$scope.job_id=null;
	$scope.params={};
	$scope.params.stdev=0.1;
	$scope.params.r=0.1;
	$scope.params.spot=100.0;
	$scope.params.strike=100.0;
	$scope.params.time=1.0;
	$scope.params.num_simulations=100000;
	resources=[window.location.origin+"/resource/norm_dist.js"];
	$scope.num_tasks=10;
	func='sum=0;for (i=0;i<arg["num_simulations"];i++) {rnd=Math.exp(nrand()*arg["stdev"]*Math.sqrt(arg["time"])+arg["time"]*(arg["r"]-arg["stdev"]*arg["stdev"]/2.0))*arg["spot"];if(rnd>arg["strike"]) sum=sum+rnd-arg["strike"];}return ([arg["num_simulations"],Math.exp(-arg["r"]*arg["time"])*sum/arg["num_simulations"]]);';

	reduce_func="(x,y)=>[(x[0])+(y[0]),(x[1]*x[0]+y[1]*y[0])/(x[0]+y[0])]";
	
	$scope.post_job=function(){
		args=[];
		for (i=0;i<$scope.num_tasks;i++) args.push($scope.params);
		post_job(func,JSON.stringify(args),reduce_func,"option pricer",resources,$scope.job_status,$scope);
	}
	
	
	$scope.job_status=function(){
		
		success=function(data){

			$scope.status=data['status'];
			if($scope.status!='completed') {setTimeout($scope.job_status, 500);}
			else{
				$.get('/job/'+$scope.job_id+'/result',function(result){
					$scope.result=(result);
					$scope.$apply();
					});
				
			}
			$scope.$apply();
		
		}


		$.get('/job/'+$scope.job_id+'/status',success);

	};
});	