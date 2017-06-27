var config = require('../../nightwatch.conf.BASIC.js');

module.exports = { // adapted from: https://git.io/vodU0
  'Page Title': function(browser) {
    browser
      .url('http://localhost:8080')
      .waitForElementVisible('body')
      .assert.title('FogLAMP Admin Dashboard')
      .saveScreenshot('page-title-test.png')
      .end();
  },

  'Success Login': function(browser) {
    browser
      .url('http://localhost:8080')
      .waitForElementVisible('body')
      .setValue('input[type=text]', 'user')
      .setValue('input[type=password]', 'password')
      .click('button[name=btn]')
      .pause(1000)
      .assert.containsText('')
      .saveScreenshot('page-title-test.png')
      .end();
  }
};