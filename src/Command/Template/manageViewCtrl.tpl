//{ControllerName}Index 控制器
app.register.controller('{ControllerName}IndexCtrl', function ($scope, $location, ${ControllerName}Services, $timeout,$ajax) {
    $scope.ctrlName = 'angularjs 控制器创建成功';
    ${ControllerName}Services.demoFunction(function (res) {
        $timeout(function () {
            $scope.ctrlName = res;
        }, 2000);
    });
    $scope.debugInfo ='loading...';
    $ajax.run(${ControllerName}Services.ajaxDebug,{},function(json){
        $scope.debugInfo = json.data;
    });
    ${ControllerName}Services._ajaxDebug(function (json) {
        $scope.debugInfo = json.data;
    });
});

app.register.service('${ControllerName}Services', function ($authUrl, $ajax) {
    return {
        ajaxDebug:{method: 'get', alisas: '{ControllerName}'},//ajax请求演示
        //演示服务
        demoFunction: function (cb) {
            cb && cb('{ControllerName}');
        },
        //ajax请求演示
        _ajaxDebug: function (paras, success, error) {
            $ajax.get('/manage/{ControllerName}/debug/', paras, function (json) {
                success && success.apply(this, arguments);
            }, function () {
                error && error.apply(this, arguments);
            });
        },
    };
});