
var app = angular.module("optionPricer", []); 
app.controller("myCtrl", function($scope) {

	$scope.job_id=null;

	resources=[];
	$scope.num_tasks=10;
	$scope.num_simulations=100000;
	func='count=0;for (i=0;i<arg;i++) {x=Math.random();y=Math.random();if (x*x+y*y<1) count++;} return [count,arg];';

	reduce_func="(x,y)=>[(x[0]+y[0]),(x[1]+y[1])]";
	
	$scope.post_job=function(){
		args=[];
		for (i=0;i<$scope.num_tasks;i++) args.push($scope.num_simulations);
		post_job(func,JSON.stringify(args),reduce_func,"PI Value",resources,$scope.job_status,$scope);
	}
	
	
	$scope.job_status=function(){
		
		success=function(data){

			$scope.status=data['status'];
			console.log(data);
			if (data['counts']==undefined || data['counts']['completed']==undefined)$scope.percentage=0.0;
			else $scope.percentage=100*data['counts']['completed']/parseFloat($scope.num_tasks);
			console.log($scope.percentage);
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