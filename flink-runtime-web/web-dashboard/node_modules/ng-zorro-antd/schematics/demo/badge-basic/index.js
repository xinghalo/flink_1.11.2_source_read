"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const schematics_1 = require("@angular-devkit/schematics");
const schematics_2 = require("@angular/cdk/schematics");
function default_1(options) {
    return schematics_1.chain([
        schematics_2.buildComponent(Object.assign({}, options), {
            template: './__path__/__name@dasherize@if-flat__/__name@dasherize__.component.html',
            stylesheet: './__path__/__name@dasherize@if-flat__/__name@dasherize__.component.__styleext__'
        })
    ]);
}
exports.default = default_1;
//# sourceMappingURL=index.js.map