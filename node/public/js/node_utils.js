var scriptCache = [];
var paths = [];
function Import(path)
{
    var index = 0;
    if((index = paths.indexOf(path)) != -1) //If we already imported this module
    {
        return;
    }

    var request, script, source;
    //var fullPath = window.location.protocol + '//' + window.location.host + '/' + path;

    request = new XMLHttpRequest();
    request.open('GET', path, false);
    request.send();

    source = request.responseText;

    eval(source);
	paths.push(path);
    return;
}

function load_res(job_id,host){
	//alert("loading reosurces for:"+job_id);
	$.ajax({
        type: "GET",
        url: host+"/job/"+job_id+"/resources",
        async: false,
        success: function(resources) {
            if(resources == "" | resources=="null" | resources==undefined ) return;
			resources=JSON.parse(resources);
			if(resources == "" | resources=="null" | resources==undefined ) return;
			if (resources.length==0) return;
			for (res in resources) Import(resources[res]);
        }
    });

}