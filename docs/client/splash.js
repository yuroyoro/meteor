Meteor.startup(function () {
  if (! amplify.store("splash")) {
    $('body').append(Meteor.ui.render(Template.splash));
  }
});

Template.splash.events = {
  'click .skip': function () {
    amplify.store("splash", true);
    $('#splash_outer').remove();
  },
  'click .submit, keypress input': function (evt) {
    if (evt.type === "click") {
      amplify.store("splash", true);
      $('#splash_outer .mask').fadeOut('fast');
      $('#splash_outer .dialog_wrapper').remove();
    }
  }
};
