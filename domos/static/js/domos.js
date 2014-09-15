/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
angular
    .module('domos', ['ngRoute'])
    .config(['$routeProvider', function($routeProvider) {
    $routeProvider
        .when('/', {
            templateUrl: '../content/homepage',
            controller: 'HomeController'
        }).when('/modules',{
            templateUrl: '../content/modules',
            controller: 'ModulesController'
        }).when('/dashi',{
            templateUrl:'../content/dashi',
            controller: 'DashiController'
        }).when('/module/:id',{
            templateUrl:'../content/module',
            controller: 'ModuleController'
        })
        .otherwise({ redirectTo: '/' });
    }])
    .controller('HomeController', [
        '$scope',
        '$http',
        function($scope, $http) {
    
    }])
    .controller('LoginController',[
        '$scope',
        '$http',
        function($scope,$http) {
            
        }
    ]).controller('NavController',[
        '$scope',
        '$http',
        '$rootScope',
        function($scope,$http,$rootScope) {
            if ($rootScope.loggedin == null) {
                $rootScope.loggedin = false;
            }
        }
    ]).controller('ModulesController',[
        '$scope',
        '$http',
        '$location',
        function($scope,$http,$location) {
            load=function(){
                $http({method:'GET',url:'/api/getmodules'})
                        .success(function(data) {
                               $scope.modules = data
                })
            }
            load()
        $scope.openmodule= function(module){
            console.log('redirecting to:'+'#/module/'+module.id)
           $location.path('module/'+module.id)
        }   
        }
    ]).controller('DashiController',[
        '$scope',
        '$http',
        function($scope,$http) {
            
        }
    ])
