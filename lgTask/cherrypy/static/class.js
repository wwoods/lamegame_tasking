/**
  John Resig's Simple Javascript Inheritance
  http://ejohn.org/blog/simple-javascript-inheritance/
  */
// Inspired by base2 and Prototype
// Modified by Walt Woods a bit for working with google closure
(function(window){
  var initializing = false
    //fnTest is used as an optimization.  If the browser we're using lets
    //us see source code of methods, then search to see if the methods we're
    //extending use super before generating replacements.
    , fnTest = eval("/xyz/.test(function(){xyz;}) ? /\\b_super\\b/ : /.*/")
    ;

  // The base Class implementation (does nothing)
  var Class = window.Class = function(){};

  // Create a new Class that inherits from this class
  Class.extend = function(prop) {
    var _super = this.prototype;

    // Instantiate a base class (but only create the instance,
    // don't run the init constructor)
    initializing = true;
    var prototype = new this();
    initializing = false;

    // Copy the properties over onto the new prototype
    for (var name in prop) {
      // Check if we're overwriting an existing function
      prototype[name] = typeof prop[name] == "function" &&
        typeof _super[name] == "function" && fnTest.test(prop[name]) ?
        (function(name, fn){
          return function() {
            var tmp = this._super;

            // Add a new ._super() method that is the same method
            // but on the super-class
            this._super = _super[name];

            // The method only need to be bound temporarily, so we
            // remove it when we're done executing
            var ret = fn.apply(this, arguments);
            this._super = tmp;

            return ret;
          };
        })(name, prop[name]) :
        prop[name];
    }

    // The dummy class constructor
    function Class() {
      // All construction is actually done in the init method
      if ( !initializing && this.init )
        this.init.apply(this, arguments);
    }

    // Populate our constructed prototype object
    Class.prototype = prototype;

    // Enforce the constructor to be what we expect
    Class.constructor = Class;

    // And make this class extendable
    Class.extend = arguments.callee;

    return Class;
  };
})(window);

//For google closure
var Class = window.Class;