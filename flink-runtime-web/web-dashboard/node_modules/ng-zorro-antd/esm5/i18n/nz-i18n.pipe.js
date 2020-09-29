/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Pipe } from '@angular/core';
import { NzI18nService } from './nz-i18n.service';
var NzI18nPipe = /** @class */ (function () {
    function NzI18nPipe(_locale) {
        this._locale = _locale;
    }
    /**
     * @param {?} path
     * @param {?=} keyValue
     * @return {?}
     */
    NzI18nPipe.prototype.transform = /**
     * @param {?} path
     * @param {?=} keyValue
     * @return {?}
     */
    function (path, keyValue) {
        return this._locale.translate(path, keyValue);
    };
    NzI18nPipe.decorators = [
        { type: Pipe, args: [{
                    name: 'nzI18n'
                },] }
    ];
    /** @nocollapse */
    NzI18nPipe.ctorParameters = function () { return [
        { type: NzI18nService }
    ]; };
    return NzI18nPipe;
}());
export { NzI18nPipe };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzI18nPipe.prototype._locale;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotaTE4bi5waXBlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImkxOG4vbnotaTE4bi5waXBlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsSUFBSSxFQUFpQixNQUFNLGVBQWUsQ0FBQztBQUVwRCxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sbUJBQW1CLENBQUM7QUFFbEQ7SUFJRSxvQkFBb0IsT0FBc0I7UUFBdEIsWUFBTyxHQUFQLE9BQU8sQ0FBZTtJQUFHLENBQUM7Ozs7OztJQUU5Qyw4QkFBUzs7Ozs7SUFBVCxVQUFVLElBQVksRUFBRSxRQUFpQjtRQUN2QyxPQUFPLElBQUksQ0FBQyxPQUFPLENBQUMsU0FBUyxDQUFDLElBQUksRUFBRSxRQUFRLENBQUMsQ0FBQztJQUNoRCxDQUFDOztnQkFSRixJQUFJLFNBQUM7b0JBQ0osSUFBSSxFQUFFLFFBQVE7aUJBQ2Y7Ozs7Z0JBSlEsYUFBYTs7SUFXdEIsaUJBQUM7Q0FBQSxBQVRELElBU0M7U0FOWSxVQUFVOzs7Ozs7SUFDVCw2QkFBOEIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBQaXBlLCBQaXBlVHJhbnNmb3JtIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IE56STE4blNlcnZpY2UgfSBmcm9tICcuL256LWkxOG4uc2VydmljZSc7XG5cbkBQaXBlKHtcbiAgbmFtZTogJ256STE4bidcbn0pXG5leHBvcnQgY2xhc3MgTnpJMThuUGlwZSBpbXBsZW1lbnRzIFBpcGVUcmFuc2Zvcm0ge1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIF9sb2NhbGU6IE56STE4blNlcnZpY2UpIHt9XG5cbiAgdHJhbnNmb3JtKHBhdGg6IHN0cmluZywga2V5VmFsdWU/OiBvYmplY3QpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLl9sb2NhbGUudHJhbnNsYXRlKHBhdGgsIGtleVZhbHVlKTtcbiAgfVxufVxuIl19