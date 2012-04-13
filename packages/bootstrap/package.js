// I don't know how to serve static files(*.png) from package.

Package.describe({
  summary: "Twitter Bootstrap(v2.0.2) for Meteor"
});

Package.on_use(function (api) {
  api.add_files([
    // "css/bootstrap-responsive.css",
    "css/bootstrap-responsive.min.css",
    // "css/bootstrap.css",
    "css/bootstrap.min.css",
    // "img/glyphicons-halflings-white.png",
    // 'img/glyphicons-halflings.png',
    // "js/bootstrap.js",
    'js/bootstrap.min.js'
  ], 'client');
});
