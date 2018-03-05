
var app = angular.module("optionPricer", []); 
app.controller("myCtrl", function($scope) {

	$scope.job_id=null;

	resources=[];
	$scope.num_tasks=20;
	$scope.num_simulations=20000;
	func='sum=0;for (i=arg[0];i<arg[1];i++) {x=i%2;sum=sum+Math.pow(-1,x)/((2*i+2)*(2*i+3)*(2*i+4));} return sum;';

	reduce_func="(x,y)=>x+y";
	
	$scope.post_job=function(){
		args=[];
		for (i=0;i<$scope.num_tasks;i++) args.push([$scope.num_simulations*i,$scope.num_simulations*(i+1)]);
		post_job(func,JSON.stringify(args),reduce_func,"PI Value Nilakantha",resources,$scope.job_status,$scope);
	}
	
	
	$scope.job_status=function(){
		
		success=function(data){

			$scope.status=data['status'];
			console.log(data);
			if (data['counts']==undefined || data['counts']['completed']==undefined)$scope.percentage=0.0;
			else $scope.percentage=100*data['counts']['completed']/parseFloat($scope.num_tasks);
			console.log($scope.percentage);
			if($scope.status!='completed') {setTimeout($scope.job_status, 500);}
			//else{
				$.get('/job/'+$scope.job_id+'/result',function(result){
					$scope.result=(result);
					$scope.$apply();
					});
				
			//}
			$scope.$apply();
		
		}


		$.get('/job/'+$scope.job_id+'/status',success);

	};
});	