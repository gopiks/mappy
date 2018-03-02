
var app = angular.module("postJob", []); 
app.controller("myCtrl", function($scope) {

	$scope.job_id=null;
	$scope.post_job=function(){
		post_job($("#func").val(),$("#args").val(),$scope.reduce_func,$scope.job_name,$scope.resources.split(","),$scope.job_status,$scope);
	}
	
	$scope.job_status=function(){
		
		success=function(data){

			$scope.status=(data)['status'];
			if($scope.status!='completed') setTimeout($scope.job_status, 500);
			$scope.$apply();
		
		}

		

		$.get('/job/'+$scope.job_id+'/status',success);

	};
});	