/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { Inject, Injectable, InjectionToken, Optional, SkipSelf } from '@angular/core';
/** @type {?} */
export var NZ_LOGGER_STATE = new InjectionToken('nz-logger-state');
// Whether print the log
var LoggerService = /** @class */ (function () {
    function LoggerService(_loggerState) {
        this._loggerState = _loggerState;
    }
    // tslint:disable-next-line:no-any
    // tslint:disable-next-line:no-any
    /**
     * @param {...?} args
     * @return {?}
     */
    LoggerService.prototype.log = 
    // tslint:disable-next-line:no-any
    /**
     * @param {...?} args
     * @return {?}
     */
    function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        if (this._loggerState) {
            console.log.apply(console, tslib_1.__spread(args));
        }
    };
    // tslint:disable-next-line:no-any
    // tslint:disable-next-line:no-any
    /**
     * @param {...?} args
     * @return {?}
     */
    LoggerService.prototype.warn = 
    // tslint:disable-next-line:no-any
    /**
     * @param {...?} args
     * @return {?}
     */
    function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        if (this._loggerState) {
            console.warn.apply(console, tslib_1.__spread(args));
        }
    };
    // tslint:disable-next-line:no-any
    // tslint:disable-next-line:no-any
    /**
     * @param {...?} args
     * @return {?}
     */
    LoggerService.prototype.error = 
    // tslint:disable-next-line:no-any
    /**
     * @param {...?} args
     * @return {?}
     */
    function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        if (this._loggerState) {
            console.error.apply(console, tslib_1.__spread(args));
        }
    };
    // tslint:disable-next-line:no-any
    // tslint:disable-next-line:no-any
    /**
     * @param {...?} args
     * @return {?}
     */
    LoggerService.prototype.info = 
    // tslint:disable-next-line:no-any
    /**
     * @param {...?} args
     * @return {?}
     */
    function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        if (this._loggerState) {
            console.log.apply(console, tslib_1.__spread(args));
        }
    };
    // tslint:disable-next-line:no-any
    // tslint:disable-next-line:no-any
    /**
     * @param {...?} args
     * @return {?}
     */
    LoggerService.prototype.debug = 
    // tslint:disable-next-line:no-any
    /**
     * @param {...?} args
     * @return {?}
     */
    function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        if (this._loggerState) {
            console.log.apply(console, tslib_1.__spread(['[NG-ZORRO-DEBUG]'], args));
        }
    };
    LoggerService.decorators = [
        { type: Injectable }
    ];
    /** @nocollapse */
    LoggerService.ctorParameters = function () { return [
        { type: Boolean, decorators: [{ type: Inject, args: [NZ_LOGGER_STATE,] }] }
    ]; };
    return LoggerService;
}());
export { LoggerService };
if (false) {
    /**
     * @type {?}
     * @private
     */
    LoggerService.prototype._loggerState;
}
/**
 * @param {?} exist
 * @param {?} loggerState
 * @return {?}
 */
export function LOGGER_SERVICE_PROVIDER_FACTORY(exist, loggerState) { return exist || new LoggerService(loggerState); }
/** @type {?} */
export var LOGGER_SERVICE_PROVIDER = {
    provide: LoggerService,
    useFactory: LOGGER_SERVICE_PROVIDER_FACTORY,
    deps: [[new Optional(), new SkipSelf(), LoggerService], NZ_LOGGER_STATE]
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibG9nZ2VyLnNlcnZpY2UuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29yZS91dGlsL2xvZ2dlci9sb2dnZXIuc2VydmljZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxNQUFNLEVBQUUsVUFBVSxFQUFFLGNBQWMsRUFBRSxRQUFRLEVBQVksUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDOztBQUVqRyxNQUFNLEtBQU8sZUFBZSxHQUFHLElBQUksY0FBYyxDQUFVLGlCQUFpQixDQUFDOztBQUU3RTtJQUVFLHVCQUE2QyxZQUFxQjtRQUFyQixpQkFBWSxHQUFaLFlBQVksQ0FBUztJQUFHLENBQUM7SUFFdEUsa0NBQWtDOzs7Ozs7SUFDbEMsMkJBQUc7Ozs7OztJQUFIO1FBQUksY0FBYzthQUFkLFVBQWMsRUFBZCxxQkFBYyxFQUFkLElBQWM7WUFBZCx5QkFBYzs7UUFDaEIsSUFBSSxJQUFJLENBQUMsWUFBWSxFQUFFO1lBQ3JCLE9BQU8sQ0FBQyxHQUFHLE9BQVgsT0FBTyxtQkFBUSxJQUFJLEdBQUU7U0FDdEI7SUFDSCxDQUFDO0lBRUQsa0NBQWtDOzs7Ozs7SUFDbEMsNEJBQUk7Ozs7OztJQUFKO1FBQUssY0FBYzthQUFkLFVBQWMsRUFBZCxxQkFBYyxFQUFkLElBQWM7WUFBZCx5QkFBYzs7UUFDakIsSUFBSSxJQUFJLENBQUMsWUFBWSxFQUFFO1lBQ3JCLE9BQU8sQ0FBQyxJQUFJLE9BQVosT0FBTyxtQkFBUyxJQUFJLEdBQUU7U0FDdkI7SUFDSCxDQUFDO0lBRUQsa0NBQWtDOzs7Ozs7SUFDbEMsNkJBQUs7Ozs7OztJQUFMO1FBQU0sY0FBYzthQUFkLFVBQWMsRUFBZCxxQkFBYyxFQUFkLElBQWM7WUFBZCx5QkFBYzs7UUFDbEIsSUFBSSxJQUFJLENBQUMsWUFBWSxFQUFFO1lBQ3JCLE9BQU8sQ0FBQyxLQUFLLE9BQWIsT0FBTyxtQkFBVSxJQUFJLEdBQUU7U0FDeEI7SUFDSCxDQUFDO0lBRUQsa0NBQWtDOzs7Ozs7SUFDbEMsNEJBQUk7Ozs7OztJQUFKO1FBQUssY0FBYzthQUFkLFVBQWMsRUFBZCxxQkFBYyxFQUFkLElBQWM7WUFBZCx5QkFBYzs7UUFDakIsSUFBSSxJQUFJLENBQUMsWUFBWSxFQUFFO1lBQ3JCLE9BQU8sQ0FBQyxHQUFHLE9BQVgsT0FBTyxtQkFBUSxJQUFJLEdBQUU7U0FDdEI7SUFDSCxDQUFDO0lBRUQsa0NBQWtDOzs7Ozs7SUFDbEMsNkJBQUs7Ozs7OztJQUFMO1FBQU0sY0FBYzthQUFkLFVBQWMsRUFBZCxxQkFBYyxFQUFkLElBQWM7WUFBZCx5QkFBYzs7UUFDbEIsSUFBSSxJQUFJLENBQUMsWUFBWSxFQUFFO1lBQ3JCLE9BQU8sQ0FBQyxHQUFHLE9BQVgsT0FBTyxvQkFBSyxrQkFBa0IsR0FBSyxJQUFJLEdBQUU7U0FDMUM7SUFDSCxDQUFDOztnQkFyQ0YsVUFBVTs7Ozs4Q0FFSSxNQUFNLFNBQUMsZUFBZTs7SUFvQ3JDLG9CQUFDO0NBQUEsQUF0Q0QsSUFzQ0M7U0FyQ1ksYUFBYTs7Ozs7O0lBQ1oscUNBQXNEOzs7Ozs7O0FBc0NwRSxNQUFNLFVBQVUsK0JBQStCLENBQUMsS0FBb0IsRUFBRSxXQUFvQixJQUFtQixPQUFPLEtBQUssSUFBSSxJQUFJLGFBQWEsQ0FBQyxXQUFXLENBQUMsQ0FBQyxDQUFDLENBQUM7O0FBRTlKLE1BQU0sS0FBTyx1QkFBdUIsR0FBYTtJQUMvQyxPQUFPLEVBQUUsYUFBYTtJQUN0QixVQUFVLEVBQUUsK0JBQStCO0lBQzNDLElBQUksRUFBRSxDQUFFLENBQUUsSUFBSSxRQUFRLEVBQUUsRUFBRSxJQUFJLFFBQVEsRUFBRSxFQUFFLGFBQWEsQ0FBRSxFQUFFLGVBQWUsQ0FBRTtDQUM3RSIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IEluamVjdCwgSW5qZWN0YWJsZSwgSW5qZWN0aW9uVG9rZW4sIE9wdGlvbmFsLCBQcm92aWRlciwgU2tpcFNlbGYgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuZXhwb3J0IGNvbnN0IE5aX0xPR0dFUl9TVEFURSA9IG5ldyBJbmplY3Rpb25Ub2tlbjxib29sZWFuPignbnotbG9nZ2VyLXN0YXRlJyk7IC8vIFdoZXRoZXIgcHJpbnQgdGhlIGxvZ1xuXG5ASW5qZWN0YWJsZSgpXG5leHBvcnQgY2xhc3MgTG9nZ2VyU2VydmljZSB7XG4gIGNvbnN0cnVjdG9yKEBJbmplY3QoTlpfTE9HR0VSX1NUQVRFKSBwcml2YXRlIF9sb2dnZXJTdGF0ZTogYm9vbGVhbikge31cblxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIGxvZyguLi5hcmdzOiBhbnlbXSk6IHZvaWQge1xuICAgIGlmICh0aGlzLl9sb2dnZXJTdGF0ZSkge1xuICAgICAgY29uc29sZS5sb2coLi4uYXJncyk7XG4gICAgfVxuICB9XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICB3YXJuKC4uLmFyZ3M6IGFueVtdKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuX2xvZ2dlclN0YXRlKSB7XG4gICAgICBjb25zb2xlLndhcm4oLi4uYXJncyk7XG4gICAgfVxuICB9XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBlcnJvciguLi5hcmdzOiBhbnlbXSk6IHZvaWQge1xuICAgIGlmICh0aGlzLl9sb2dnZXJTdGF0ZSkge1xuICAgICAgY29uc29sZS5lcnJvciguLi5hcmdzKTtcbiAgICB9XG4gIH1cblxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIGluZm8oLi4uYXJnczogYW55W10pOiB2b2lkIHtcbiAgICBpZiAodGhpcy5fbG9nZ2VyU3RhdGUpIHtcbiAgICAgIGNvbnNvbGUubG9nKC4uLmFyZ3MpO1xuICAgIH1cbiAgfVxuXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgZGVidWcoLi4uYXJnczogYW55W10pOiB2b2lkIHtcbiAgICBpZiAodGhpcy5fbG9nZ2VyU3RhdGUpIHtcbiAgICAgIGNvbnNvbGUubG9nKCdbTkctWk9SUk8tREVCVUddJywgLi4uYXJncyk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBMT0dHRVJfU0VSVklDRV9QUk9WSURFUl9GQUNUT1JZKGV4aXN0OiBMb2dnZXJTZXJ2aWNlLCBsb2dnZXJTdGF0ZTogYm9vbGVhbik6IExvZ2dlclNlcnZpY2UgeyByZXR1cm4gZXhpc3QgfHwgbmV3IExvZ2dlclNlcnZpY2UobG9nZ2VyU3RhdGUpOyB9XG5cbmV4cG9ydCBjb25zdCBMT0dHRVJfU0VSVklDRV9QUk9WSURFUjogUHJvdmlkZXIgPSB7XG4gIHByb3ZpZGU6IExvZ2dlclNlcnZpY2UsXG4gIHVzZUZhY3Rvcnk6IExPR0dFUl9TRVJWSUNFX1BST1ZJREVSX0ZBQ1RPUlksXG4gIGRlcHM6IFsgWyBuZXcgT3B0aW9uYWwoKSwgbmV3IFNraXBTZWxmKCksIExvZ2dlclNlcnZpY2UgXSwgTlpfTE9HR0VSX1NUQVRFIF1cbn07XG4iXX0=