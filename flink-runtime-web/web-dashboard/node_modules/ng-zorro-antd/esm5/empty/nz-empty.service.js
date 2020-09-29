/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Inject, Injectable, Optional, TemplateRef, Type } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { NZ_DEFAULT_EMPTY_CONTENT } from './nz-empty-config';
import { getEmptyContentTypeError } from './nz-empty-error';
import * as i0 from "@angular/core";
import * as i1 from "./public-api";
/**
 * @template T
 */
var NzEmptyService = /** @class */ (function () {
    function NzEmptyService(defaultEmptyContent) {
        this.defaultEmptyContent = defaultEmptyContent;
        this.userDefaultContent$ = new BehaviorSubject(undefined);
        if (this.defaultEmptyContent) {
            this.userDefaultContent$.next(this.defaultEmptyContent);
        }
    }
    /**
     * @param {?=} content
     * @return {?}
     */
    NzEmptyService.prototype.setDefaultContent = /**
     * @param {?=} content
     * @return {?}
     */
    function (content) {
        if (typeof content === 'string' ||
            content === undefined ||
            content === null ||
            content instanceof TemplateRef ||
            content instanceof Type) {
            this.userDefaultContent$.next(content);
        }
        else {
            throw getEmptyContentTypeError(content);
        }
    };
    /**
     * @return {?}
     */
    NzEmptyService.prototype.resetDefault = /**
     * @return {?}
     */
    function () {
        this.userDefaultContent$.next(undefined);
    };
    NzEmptyService.decorators = [
        { type: Injectable, args: [{
                    providedIn: 'root'
                },] }
    ];
    /** @nocollapse */
    NzEmptyService.ctorParameters = function () { return [
        { type: Type, decorators: [{ type: Inject, args: [NZ_DEFAULT_EMPTY_CONTENT,] }, { type: Optional }] }
    ]; };
    /** @nocollapse */ NzEmptyService.ngInjectableDef = i0.defineInjectable({ factory: function NzEmptyService_Factory() { return new i1.NzEmptyService(i0.inject(i1.NZ_DEFAULT_EMPTY_CONTENT, 8)); }, token: i1.NzEmptyService, providedIn: "root" });
    return NzEmptyService;
}());
export { NzEmptyService };
if (false) {
    /** @type {?} */
    NzEmptyService.prototype.userDefaultContent$;
    /**
     * @type {?}
     * @private
     */
    NzEmptyService.prototype.defaultEmptyContent;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZW1wdHkuc2VydmljZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJlbXB0eS9uei1lbXB0eS5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsTUFBTSxFQUFFLFVBQVUsRUFBRSxRQUFRLEVBQUUsV0FBVyxFQUFFLElBQUksRUFBRSxNQUFNLGVBQWUsQ0FBQztBQUNoRixPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQ3ZDLE9BQU8sRUFBd0Isd0JBQXdCLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUNuRixPQUFPLEVBQUUsd0JBQXdCLEVBQUUsTUFBTSxrQkFBa0IsQ0FBQzs7Ozs7O0FBRTVEO0lBT0Usd0JBQWtFLG1CQUE0QjtRQUE1Qix3QkFBbUIsR0FBbkIsbUJBQW1CLENBQVM7UUFGOUYsd0JBQW1CLEdBQUcsSUFBSSxlQUFlLENBQW1DLFNBQVMsQ0FBQyxDQUFDO1FBR3JGLElBQUksSUFBSSxDQUFDLG1CQUFtQixFQUFFO1lBQzVCLElBQUksQ0FBQyxtQkFBbUIsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLG1CQUFtQixDQUFDLENBQUM7U0FDekQ7SUFDSCxDQUFDOzs7OztJQUVELDBDQUFpQjs7OztJQUFqQixVQUFrQixPQUE4QjtRQUM5QyxJQUNFLE9BQU8sT0FBTyxLQUFLLFFBQVE7WUFDM0IsT0FBTyxLQUFLLFNBQVM7WUFDckIsT0FBTyxLQUFLLElBQUk7WUFDaEIsT0FBTyxZQUFZLFdBQVc7WUFDOUIsT0FBTyxZQUFZLElBQUksRUFDdkI7WUFDQSxJQUFJLENBQUMsbUJBQW1CLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxDQUFDO1NBQ3hDO2FBQU07WUFDTCxNQUFNLHdCQUF3QixDQUFDLE9BQU8sQ0FBQyxDQUFDO1NBQ3pDO0lBQ0gsQ0FBQzs7OztJQUVELHFDQUFZOzs7SUFBWjtRQUNFLElBQUksQ0FBQyxtQkFBbUIsQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLENBQUM7SUFDM0MsQ0FBQzs7Z0JBN0JGLFVBQVUsU0FBQztvQkFDVixVQUFVLEVBQUUsTUFBTTtpQkFDbkI7Ozs7Z0JBUG1ELElBQUksdUJBWXpDLE1BQU0sU0FBQyx3QkFBd0IsY0FBRyxRQUFROzs7eUJBWnpEO0NBbUNDLEFBOUJELElBOEJDO1NBMUJZLGNBQWM7OztJQUN6Qiw2Q0FBdUY7Ozs7O0lBRTNFLDZDQUFrRiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IEluamVjdCwgSW5qZWN0YWJsZSwgT3B0aW9uYWwsIFRlbXBsYXRlUmVmLCBUeXBlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBCZWhhdmlvclN1YmplY3QgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IE56RW1wdHlDdXN0b21Db250ZW50LCBOWl9ERUZBVUxUX0VNUFRZX0NPTlRFTlQgfSBmcm9tICcuL256LWVtcHR5LWNvbmZpZyc7XG5pbXBvcnQgeyBnZXRFbXB0eUNvbnRlbnRUeXBlRXJyb3IgfSBmcm9tICcuL256LWVtcHR5LWVycm9yJztcblxuQEluamVjdGFibGUoe1xuICBwcm92aWRlZEluOiAncm9vdCdcbn0pXG4vLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG5leHBvcnQgY2xhc3MgTnpFbXB0eVNlcnZpY2U8VCA9IGFueT4ge1xuICB1c2VyRGVmYXVsdENvbnRlbnQkID0gbmV3IEJlaGF2aW9yU3ViamVjdDxOekVtcHR5Q3VzdG9tQ29udGVudCB8IHVuZGVmaW5lZD4odW5kZWZpbmVkKTtcblxuICBjb25zdHJ1Y3RvcihASW5qZWN0KE5aX0RFRkFVTFRfRU1QVFlfQ09OVEVOVCkgQE9wdGlvbmFsKCkgcHJpdmF0ZSBkZWZhdWx0RW1wdHlDb250ZW50OiBUeXBlPFQ+KSB7XG4gICAgaWYgKHRoaXMuZGVmYXVsdEVtcHR5Q29udGVudCkge1xuICAgICAgdGhpcy51c2VyRGVmYXVsdENvbnRlbnQkLm5leHQodGhpcy5kZWZhdWx0RW1wdHlDb250ZW50KTtcbiAgICB9XG4gIH1cblxuICBzZXREZWZhdWx0Q29udGVudChjb250ZW50PzogTnpFbXB0eUN1c3RvbUNvbnRlbnQpOiB2b2lkIHtcbiAgICBpZiAoXG4gICAgICB0eXBlb2YgY29udGVudCA9PT0gJ3N0cmluZycgfHxcbiAgICAgIGNvbnRlbnQgPT09IHVuZGVmaW5lZCB8fFxuICAgICAgY29udGVudCA9PT0gbnVsbCB8fFxuICAgICAgY29udGVudCBpbnN0YW5jZW9mIFRlbXBsYXRlUmVmIHx8XG4gICAgICBjb250ZW50IGluc3RhbmNlb2YgVHlwZVxuICAgICkge1xuICAgICAgdGhpcy51c2VyRGVmYXVsdENvbnRlbnQkLm5leHQoY29udGVudCk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRocm93IGdldEVtcHR5Q29udGVudFR5cGVFcnJvcihjb250ZW50KTtcbiAgICB9XG4gIH1cblxuICByZXNldERlZmF1bHQoKTogdm9pZCB7XG4gICAgdGhpcy51c2VyRGVmYXVsdENvbnRlbnQkLm5leHQodW5kZWZpbmVkKTtcbiAgfVxufVxuIl19