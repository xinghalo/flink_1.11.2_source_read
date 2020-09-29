/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Pipe } from '@angular/core';
import { NzI18nService } from './nz-i18n.service';
export class NzI18nPipe {
    /**
     * @param {?} _locale
     */
    constructor(_locale) {
        this._locale = _locale;
    }
    /**
     * @param {?} path
     * @param {?=} keyValue
     * @return {?}
     */
    transform(path, keyValue) {
        return this._locale.translate(path, keyValue);
    }
}
NzI18nPipe.decorators = [
    { type: Pipe, args: [{
                name: 'nzI18n'
            },] }
];
/** @nocollapse */
NzI18nPipe.ctorParameters = () => [
    { type: NzI18nService }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzI18nPipe.prototype._locale;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotaTE4bi5waXBlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImkxOG4vbnotaTE4bi5waXBlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsSUFBSSxFQUFpQixNQUFNLGVBQWUsQ0FBQztBQUVwRCxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sbUJBQW1CLENBQUM7QUFLbEQsTUFBTSxPQUFPLFVBQVU7Ozs7SUFDckIsWUFBb0IsT0FBc0I7UUFBdEIsWUFBTyxHQUFQLE9BQU8sQ0FBZTtJQUFHLENBQUM7Ozs7OztJQUU5QyxTQUFTLENBQUMsSUFBWSxFQUFFLFFBQWlCO1FBQ3ZDLE9BQU8sSUFBSSxDQUFDLE9BQU8sQ0FBQyxTQUFTLENBQUMsSUFBSSxFQUFFLFFBQVEsQ0FBQyxDQUFDO0lBQ2hELENBQUM7OztZQVJGLElBQUksU0FBQztnQkFDSixJQUFJLEVBQUUsUUFBUTthQUNmOzs7O1lBSlEsYUFBYTs7Ozs7OztJQU1SLDZCQUE4QiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IFBpcGUsIFBpcGVUcmFuc2Zvcm0gfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgTnpJMThuU2VydmljZSB9IGZyb20gJy4vbnotaTE4bi5zZXJ2aWNlJztcblxuQFBpcGUoe1xuICBuYW1lOiAnbnpJMThuJ1xufSlcbmV4cG9ydCBjbGFzcyBOekkxOG5QaXBlIGltcGxlbWVudHMgUGlwZVRyYW5zZm9ybSB7XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgX2xvY2FsZTogTnpJMThuU2VydmljZSkge31cblxuICB0cmFuc2Zvcm0ocGF0aDogc3RyaW5nLCBrZXlWYWx1ZT86IG9iamVjdCk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHRoaXMuX2xvY2FsZS50cmFuc2xhdGUocGF0aCwga2V5VmFsdWUpO1xuICB9XG59XG4iXX0=