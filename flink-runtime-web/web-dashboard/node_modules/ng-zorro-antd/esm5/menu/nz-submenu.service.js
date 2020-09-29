/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Injectable, Optional, SkipSelf } from '@angular/core';
import { combineLatest, BehaviorSubject, Subject } from 'rxjs';
import { auditTime, distinctUntilChanged, map, tap } from 'rxjs/operators';
import { NzMenuService } from './nz-menu.service';
var NzSubmenuService = /** @class */ (function () {
    function NzSubmenuService(nzHostSubmenuService, nzMenuService) {
        var _this = this;
        this.nzHostSubmenuService = nzHostSubmenuService;
        this.nzMenuService = nzMenuService;
        this.disabled = false;
        this.mode = 'vertical';
        this.mode$ = this.nzMenuService.mode$.pipe(map((/**
         * @param {?} mode
         * @return {?}
         */
        function (mode) {
            if (mode === 'inline') {
                return 'inline';
            }
            else if (mode === 'vertical' || _this.nzHostSubmenuService) {
                return 'vertical';
            }
            else {
                return 'horizontal';
            }
        })), tap((/**
         * @param {?} mode
         * @return {?}
         */
        function (mode) { return (_this.mode = (/** @type {?} */ (mode))); })));
        this.level = 1;
        this.level$ = new BehaviorSubject(1);
        this.subMenuOpen$ = new BehaviorSubject(false);
        this.open$ = new BehaviorSubject(false);
        this.mouseEnterLeave$ = new Subject();
        this.menuOpen$ = combineLatest(this.subMenuOpen$, this.mouseEnterLeave$).pipe(map((/**
         * @param {?} value
         * @return {?}
         */
        function (value) { return value[0] || value[1]; })), auditTime(150), distinctUntilChanged(), tap((/**
         * @param {?} data
         * @return {?}
         */
        function (data) {
            _this.setOpenState(data);
            if (_this.nzHostSubmenuService) {
                _this.nzHostSubmenuService.subMenuOpen$.next(data);
            }
        })));
        if (this.nzHostSubmenuService) {
            this.setLevel(this.nzHostSubmenuService.level + 1);
        }
    }
    /**
     * @param {?} value
     * @return {?}
     */
    NzSubmenuService.prototype.setOpenState = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.open$.next(value);
    };
    /**
     * @return {?}
     */
    NzSubmenuService.prototype.onMenuItemClick = /**
     * @return {?}
     */
    function () {
        this.setMouseEnterState(false);
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzSubmenuService.prototype.setLevel = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.level$.next(value);
        this.level = value;
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzSubmenuService.prototype.setMouseEnterState = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        if ((this.mode === 'horizontal' || this.mode === 'vertical' || this.nzMenuService.isInDropDown) && !this.disabled) {
            this.mouseEnterLeave$.next(value);
        }
    };
    NzSubmenuService.decorators = [
        { type: Injectable }
    ];
    /** @nocollapse */
    NzSubmenuService.ctorParameters = function () { return [
        { type: NzSubmenuService, decorators: [{ type: SkipSelf }, { type: Optional }] },
        { type: NzMenuService }
    ]; };
    return NzSubmenuService;
}());
export { NzSubmenuService };
if (false) {
    /** @type {?} */
    NzSubmenuService.prototype.disabled;
    /** @type {?} */
    NzSubmenuService.prototype.mode;
    /** @type {?} */
    NzSubmenuService.prototype.mode$;
    /** @type {?} */
    NzSubmenuService.prototype.level;
    /** @type {?} */
    NzSubmenuService.prototype.level$;
    /** @type {?} */
    NzSubmenuService.prototype.subMenuOpen$;
    /** @type {?} */
    NzSubmenuService.prototype.open$;
    /** @type {?} */
    NzSubmenuService.prototype.mouseEnterLeave$;
    /** @type {?} */
    NzSubmenuService.prototype.menuOpen$;
    /**
     * @type {?}
     * @private
     */
    NzSubmenuService.prototype.nzHostSubmenuService;
    /** @type {?} */
    NzSubmenuService.prototype.nzMenuService;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc3VibWVudS5zZXJ2aWNlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbIm1lbnUvbnotc3VibWVudS5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsVUFBVSxFQUFFLFFBQVEsRUFBRSxRQUFRLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFDL0QsT0FBTyxFQUFFLGFBQWEsRUFBRSxlQUFlLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQy9ELE9BQU8sRUFBRSxTQUFTLEVBQUUsb0JBQW9CLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBRTNFLE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUVsRDtJQW9ERSwwQkFDa0Msb0JBQXNDLEVBQy9ELGFBQTRCO1FBRnJDLGlCQU9DO1FBTmlDLHlCQUFvQixHQUFwQixvQkFBb0IsQ0FBa0I7UUFDL0Qsa0JBQWEsR0FBYixhQUFhLENBQWU7UUFwRHJDLGFBQVEsR0FBRyxLQUFLLENBQUM7UUFDakIsU0FBSSxHQUF1QixVQUFVLENBQUM7UUFDdEMsVUFBSyxHQUFHLElBQUksQ0FBQyxhQUFhLENBQUMsS0FBSyxDQUFDLElBQUksQ0FDbkMsR0FBRzs7OztRQUFDLFVBQUEsSUFBSTtZQUNOLElBQUksSUFBSSxLQUFLLFFBQVEsRUFBRTtnQkFDckIsT0FBTyxRQUFRLENBQUM7YUFDakI7aUJBQU0sSUFBSSxJQUFJLEtBQUssVUFBVSxJQUFJLEtBQUksQ0FBQyxvQkFBb0IsRUFBRTtnQkFDM0QsT0FBTyxVQUFVLENBQUM7YUFDbkI7aUJBQU07Z0JBQ0wsT0FBTyxZQUFZLENBQUM7YUFDckI7UUFDSCxDQUFDLEVBQUMsRUFDRixHQUFHOzs7O1FBQUMsVUFBQSxJQUFJLElBQUksT0FBQSxDQUFDLEtBQUksQ0FBQyxJQUFJLEdBQUcsbUJBQUEsSUFBSSxFQUFzQixDQUFDLEVBQXhDLENBQXdDLEVBQUMsQ0FDdEQsQ0FBQztRQUNGLFVBQUssR0FBRyxDQUFDLENBQUM7UUFDVixXQUFNLEdBQUcsSUFBSSxlQUFlLENBQVMsQ0FBQyxDQUFDLENBQUM7UUFDeEMsaUJBQVksR0FBRyxJQUFJLGVBQWUsQ0FBVSxLQUFLLENBQUMsQ0FBQztRQUNuRCxVQUFLLEdBQUcsSUFBSSxlQUFlLENBQVUsS0FBSyxDQUFDLENBQUM7UUFDNUMscUJBQWdCLEdBQUcsSUFBSSxPQUFPLEVBQVcsQ0FBQztRQUMxQyxjQUFTLEdBQUcsYUFBYSxDQUFDLElBQUksQ0FBQyxZQUFZLEVBQUUsSUFBSSxDQUFDLGdCQUFnQixDQUFDLENBQUMsSUFBSSxDQUN0RSxHQUFHOzs7O1FBQUMsVUFBQSxLQUFLLElBQUksT0FBQSxLQUFLLENBQUMsQ0FBQyxDQUFDLElBQUksS0FBSyxDQUFDLENBQUMsQ0FBQyxFQUFwQixDQUFvQixFQUFDLEVBQ2xDLFNBQVMsQ0FBQyxHQUFHLENBQUMsRUFDZCxvQkFBb0IsRUFBRSxFQUN0QixHQUFHOzs7O1FBQUMsVUFBQSxJQUFJO1lBQ04sS0FBSSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUN4QixJQUFJLEtBQUksQ0FBQyxvQkFBb0IsRUFBRTtnQkFDN0IsS0FBSSxDQUFDLG9CQUFvQixDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUM7YUFDbkQ7UUFDSCxDQUFDLEVBQUMsQ0FDSCxDQUFDO1FBeUJBLElBQUksSUFBSSxDQUFDLG9CQUFvQixFQUFFO1lBQzdCLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLG9CQUFvQixDQUFDLEtBQUssR0FBRyxDQUFDLENBQUMsQ0FBQztTQUNwRDtJQUNILENBQUM7Ozs7O0lBMUJELHVDQUFZOzs7O0lBQVosVUFBYSxLQUFjO1FBQ3pCLElBQUksQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDO0lBQ3pCLENBQUM7Ozs7SUFFRCwwQ0FBZTs7O0lBQWY7UUFDRSxJQUFJLENBQUMsa0JBQWtCLENBQUMsS0FBSyxDQUFDLENBQUM7SUFDakMsQ0FBQzs7Ozs7SUFFRCxtQ0FBUTs7OztJQUFSLFVBQVMsS0FBYTtRQUNwQixJQUFJLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUN4QixJQUFJLENBQUMsS0FBSyxHQUFHLEtBQUssQ0FBQztJQUNyQixDQUFDOzs7OztJQUVELDZDQUFrQjs7OztJQUFsQixVQUFtQixLQUFjO1FBQy9CLElBQUksQ0FBQyxJQUFJLENBQUMsSUFBSSxLQUFLLFlBQVksSUFBSSxJQUFJLENBQUMsSUFBSSxLQUFLLFVBQVUsSUFBSSxJQUFJLENBQUMsYUFBYSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsRUFBRTtZQUNqSCxJQUFJLENBQUMsZ0JBQWdCLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDO1NBQ25DO0lBQ0gsQ0FBQzs7Z0JBbERGLFVBQVU7Ozs7Z0JBcUQrQyxnQkFBZ0IsdUJBQXJFLFFBQVEsWUFBSSxRQUFRO2dCQXZEaEIsYUFBYTs7SUE4RHRCLHVCQUFDO0NBQUEsQUE1REQsSUE0REM7U0EzRFksZ0JBQWdCOzs7SUFDM0Isb0NBQWlCOztJQUNqQixnQ0FBc0M7O0lBQ3RDLGlDQVdFOztJQUNGLGlDQUFVOztJQUNWLGtDQUF3Qzs7SUFDeEMsd0NBQW1EOztJQUNuRCxpQ0FBNEM7O0lBQzVDLDRDQUEwQzs7SUFDMUMscUNBVUU7Ozs7O0lBc0JBLGdEQUFzRTs7SUFDdEUseUNBQW1DIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgSW5qZWN0YWJsZSwgT3B0aW9uYWwsIFNraXBTZWxmIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBjb21iaW5lTGF0ZXN0LCBCZWhhdmlvclN1YmplY3QsIFN1YmplY3QgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IGF1ZGl0VGltZSwgZGlzdGluY3RVbnRpbENoYW5nZWQsIG1hcCwgdGFwIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgTnpEaXJlY3Rpb25WSElUeXBlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9kaXJlY3Rpb24nO1xuaW1wb3J0IHsgTnpNZW51U2VydmljZSB9IGZyb20gJy4vbnotbWVudS5zZXJ2aWNlJztcblxuQEluamVjdGFibGUoKVxuZXhwb3J0IGNsYXNzIE56U3VibWVudVNlcnZpY2Uge1xuICBkaXNhYmxlZCA9IGZhbHNlO1xuICBtb2RlOiBOekRpcmVjdGlvblZISVR5cGUgPSAndmVydGljYWwnO1xuICBtb2RlJCA9IHRoaXMubnpNZW51U2VydmljZS5tb2RlJC5waXBlKFxuICAgIG1hcChtb2RlID0+IHtcbiAgICAgIGlmIChtb2RlID09PSAnaW5saW5lJykge1xuICAgICAgICByZXR1cm4gJ2lubGluZSc7XG4gICAgICB9IGVsc2UgaWYgKG1vZGUgPT09ICd2ZXJ0aWNhbCcgfHwgdGhpcy5uekhvc3RTdWJtZW51U2VydmljZSkge1xuICAgICAgICByZXR1cm4gJ3ZlcnRpY2FsJztcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHJldHVybiAnaG9yaXpvbnRhbCc7XG4gICAgICB9XG4gICAgfSksXG4gICAgdGFwKG1vZGUgPT4gKHRoaXMubW9kZSA9IG1vZGUgYXMgTnpEaXJlY3Rpb25WSElUeXBlKSlcbiAgKTtcbiAgbGV2ZWwgPSAxO1xuICBsZXZlbCQgPSBuZXcgQmVoYXZpb3JTdWJqZWN0PG51bWJlcj4oMSk7XG4gIHN1Yk1lbnVPcGVuJCA9IG5ldyBCZWhhdmlvclN1YmplY3Q8Ym9vbGVhbj4oZmFsc2UpO1xuICBvcGVuJCA9IG5ldyBCZWhhdmlvclN1YmplY3Q8Ym9vbGVhbj4oZmFsc2UpO1xuICBtb3VzZUVudGVyTGVhdmUkID0gbmV3IFN1YmplY3Q8Ym9vbGVhbj4oKTtcbiAgbWVudU9wZW4kID0gY29tYmluZUxhdGVzdCh0aGlzLnN1Yk1lbnVPcGVuJCwgdGhpcy5tb3VzZUVudGVyTGVhdmUkKS5waXBlKFxuICAgIG1hcCh2YWx1ZSA9PiB2YWx1ZVswXSB8fCB2YWx1ZVsxXSksXG4gICAgYXVkaXRUaW1lKDE1MCksXG4gICAgZGlzdGluY3RVbnRpbENoYW5nZWQoKSxcbiAgICB0YXAoZGF0YSA9PiB7XG4gICAgICB0aGlzLnNldE9wZW5TdGF0ZShkYXRhKTtcbiAgICAgIGlmICh0aGlzLm56SG9zdFN1Ym1lbnVTZXJ2aWNlKSB7XG4gICAgICAgIHRoaXMubnpIb3N0U3VibWVudVNlcnZpY2Uuc3ViTWVudU9wZW4kLm5leHQoZGF0YSk7XG4gICAgICB9XG4gICAgfSlcbiAgKTtcblxuICBzZXRPcGVuU3RhdGUodmFsdWU6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLm9wZW4kLm5leHQodmFsdWUpO1xuICB9XG5cbiAgb25NZW51SXRlbUNsaWNrKCk6IHZvaWQge1xuICAgIHRoaXMuc2V0TW91c2VFbnRlclN0YXRlKGZhbHNlKTtcbiAgfVxuXG4gIHNldExldmVsKHZhbHVlOiBudW1iZXIpOiB2b2lkIHtcbiAgICB0aGlzLmxldmVsJC5uZXh0KHZhbHVlKTtcbiAgICB0aGlzLmxldmVsID0gdmFsdWU7XG4gIH1cblxuICBzZXRNb3VzZUVudGVyU3RhdGUodmFsdWU6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICBpZiAoKHRoaXMubW9kZSA9PT0gJ2hvcml6b250YWwnIHx8IHRoaXMubW9kZSA9PT0gJ3ZlcnRpY2FsJyB8fCB0aGlzLm56TWVudVNlcnZpY2UuaXNJbkRyb3BEb3duKSAmJiAhdGhpcy5kaXNhYmxlZCkge1xuICAgICAgdGhpcy5tb3VzZUVudGVyTGVhdmUkLm5leHQodmFsdWUpO1xuICAgIH1cbiAgfVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIEBTa2lwU2VsZigpIEBPcHRpb25hbCgpIHByaXZhdGUgbnpIb3N0U3VibWVudVNlcnZpY2U6IE56U3VibWVudVNlcnZpY2UsXG4gICAgcHVibGljIG56TWVudVNlcnZpY2U6IE56TWVudVNlcnZpY2VcbiAgKSB7XG4gICAgaWYgKHRoaXMubnpIb3N0U3VibWVudVNlcnZpY2UpIHtcbiAgICAgIHRoaXMuc2V0TGV2ZWwodGhpcy5uekhvc3RTdWJtZW51U2VydmljZS5sZXZlbCArIDEpO1xuICAgIH1cbiAgfVxufVxuIl19