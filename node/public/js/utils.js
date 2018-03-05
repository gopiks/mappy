pub_key="pub-c-a2ec90e1-9820-419f-a601-d8f27a595636";
sub_key="sub-c-037fce1e-0c77-11e8-941f-7e2964818bdb";
pubnub = new PubNub({
	subscribeKey: sub_key,
	publishKey: pub_key,
	ssl: true
});


post_job=function(func,args,reduce_func,job_name,resources,job_status,scope){
		
	success=function(data){
		scope['job_id']=(data)	;
		job_status();
		pubnub.publish(
			{
				message: {
					job_id:(data),
					host:window.location.origin
				},
				channel: 'test_project'
			},
			function (status, response) {
				if (status.error) {
					alert("error:"+response);
					console.log(status)
				} else {
					console.log("message Published w/ timetoken", response.timetoken)
				}
			}
		);
	}


	data={'func':func,'args':args,'reduce_func':reduce_func,'job_name':job_name,'resources':resources};
	$.ajax({
				url: "/job",
				type: "POST",
				data: JSON.stringify(data),
				contentType: "application/json; charset=utf-8",
				success: success
			});
	try {
		scope.$apply();
	}catch(e){
	}
	
	
} 