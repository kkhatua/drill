<#--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->
<#macro page_head>
</#macro>

<#macro page_body>
</#macro>

<#macro page_html>
  <!DOCTYPE html>
  <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta http-equiv="X-UA-Compatible" content="IE=edge">
      <script type="text/javascript" language="javascript" src="${rootDepth}/static/js/jquery-3.2.1.min.js"></script>
      <link rel='shortcut icon' href="${rootDepth}/static/img/drill.ico">
      <link href="${rootDepth}/static/css/bootstrap.min.css" rel="stylesheet">
      <script src="${rootDepth}/static/js/bootstrap.min.js"></script>

      <!-- HTML5 shim and Respond.js IE8 support of HTML5 elements and media queries -->
      <!--[if lt IE 9]>
        <script src="static/js/html5shiv.js"></script>
        <script src="static/js/1.4.2/respond.min.js"></script>
      <![endif]-->

    <!-- Using HTML5 WebSession to store root path. All links will be in reference to this -->
    <script>
    // Discover Drill Service Root
    function discoverServiceRoot() {
      jQuery.ajaxSetup({async:false});
      var absUrl = new URL(location.protocol+"//"+location.host + location.pathname.replace(/\/$/, "") + "/${rootDepth}/..");
      console.log("absUrl = " + absUrl.href);
	  var testPath = absUrl.href+"state";
      console.log("testPath = " + testPath);
      $.get(testPath, function(data, status) {
        console.log("Yahoooo:: Data: " + data + " ["+status+"] for => " + testPath);
        sessionStorage.drillSvcRoot = absUrl.href;
      });
      jQuery.ajaxSetup({async:true});
      //dBug::
      console.log("Drill BASE >-> " + sessionStorage.drillSvcRoot);
	}

    var testPathPrefix = location.protocol+"//"+location.host;
    if (typeof(Storage) !== "undefined") {
	  if (!sessionStorage.drillSvcRoot || sessionStorage.drillSvcRoot === "undefined" || !sessionStorage.drillSvcRoot.startsWith(testPathPrefix)) {
        discoverServiceRoot();
      }
    } else {
      alert("Sorry, your browser does not support web storage. Please use an HTML5+ browser");
    }

    // Translate to absolute paths for Javascript
    function makePath(path) {
	  //Rediscover if not
	  if (sessionStorage.drillSvcRoot === "undefined") {
	    discoverServiceRoot();
	  }
      var newPath = sessionStorage.drillSvcRoot + path;
      if (path.startsWith("/")) {
        newPath = sessionStorage.drillSvcRoot + path.substr(1);
      }
      //dBug::
      console.log("mkpath:: "+ newPath)
      return newPath;
    }
    </script>

      <title>Apache Drill</title>
      <@page_head/>
    </head>

    <body role="document">

      <div class="navbar navbar-inverse navbar-fixed-top" role="navigation">
        <div class="container-fluid">
          <div class="navbar-header">
            <button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
              <span class="sr-only">Toggle navigation</span>
              <span class="icon-bar"></span>
              <span class="icon-bar"></span>
              <span class="icon-bar"></span>
            </button>
            <a class='navbar-brand' href='${rootDepth}/'>Apache Drill</a>
          </div>
          <div class='navbar-collapse collapse'>
            <#if showControls == true>
            <ul class='nav navbar-nav'>
              <li><a href='${rootDepth}/query'>Query</a></li>
              <li><a href='${rootDepth}/profiles'>Profiles</a></li>
              <#if showStorage == true>
              <li><a href='${rootDepth}/storage'>Storage</a></li>
              </#if>
              <li><a href='${rootDepth}/metrics'>Metrics</a></li>
              <#if showThreads == true>
              <li><a href='${rootDepth}/threads'>Threads</a></li>
              </#if>
              <#if showLogs == true>
                  <li><a href='${rootDepth}/logs'>Logs</a></li>
              </#if>
            </ul>
            </#if>
            <ul class='nav navbar-nav navbar-right'>
              <#if showOptions == true>
              <li><a href='${rootDepth}/options'>Options</a></li>
              </#if>
              <li><a href='http://drill.apache.org/docs/'>Documentation</a>
              <#if showLogin == true >
              <li><a href='${rootDepth}/mainLogin'>Log In</a>
              </#if>
              <#if showLogout == true >
              <li><a href='${rootDepth}/logout'>Log Out (${loggedInUserName})</a>
              </#if>
            </ul>
          </div>
        </div>
      </div>

      <div class="container-fluid" role="main">
        <@page_body/>
      </div>
    </body>
  </html>
</#macro>
