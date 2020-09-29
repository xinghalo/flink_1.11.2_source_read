/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Injectable, Optional, SkipSelf } from '@angular/core';
import { combineLatest, BehaviorSubject, Subject } from 'rxjs';
import { auditTime, distinctUntilChanged, map, tap } from 'rxjs/operators';
import { NzMenuService } from './nz-menu.service';
export class NzSubmenuService {
    /**
     * @param {?} nzHostSubmenuService
     * @param {?} nzMenuService
     */
    constructor(nzHostSubmenuService, nzMenuService) {
        this.nzHostSubmenuService = nzHostSubmenuService;
        this.nzMenuService = nzMenuService;
        this.disabled = false;
        this.mode = 'vertical';
        this.mode$ = this.nzMenuService.mode$.pipe(map((/**
         * @param {?} mode
         * @return {?}
         */
        mode => {
            if (mode === 'inline') {
                return 'inline';
            }
            else if (mode === 'vertical' || this.nzHostSubmenuService) {
                return 'vertical';
            }
            else {
                return 'horizontal';
            }
        })), tap((/**
         * @param {?} mode
         * @return {?}
         */
        mode => (this.mode = (/** @type {?} */ (mode))))));
        this.level = 1;
        this.level$ = new BehaviorSubject(1);
        this.subMenuOpen$ = new BehaviorSubject(false);
        this.open$ = new BehaviorSubject(false);
        this.mouseEnterLeave$ = new Subject();
        this.menuOpen$ = combineLatest(this.subMenuOpen$, this.mouseEnterLeave$).pipe(map((/**
         * @param {?} value
         * @return {?}
         */
        value => value[0] || value[1])), auditTime(150), distinctUntilChanged(), tap((/**
         * @param {?} data
         * @return {?}
         */
        data => {
            this.setOpenState(data);
            if (this.nzHostSubmenuService) {
                this.nzHostSubmenuService.subMenuOpen$.next(data);
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
    setOpenState(value) {
        this.open$.next(value);
    }
    /**
     * @return {?}
     */
    onMenuItemClick() {
        this.setMouseEnterState(false);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    setLevel(value) {
        this.level$.next(value);
        this.level = value;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    setMouseEnterState(value) {
        if ((this.mode === 'horizontal' || this.mode === 'vertical' || this.nzMenuService.isInDropDown) && !this.disabled) {
            this.mouseEnterLeave$.next(value);
        }
    }
}
NzSubmenuService.decorators = [
    { type: Injectable }
];
/** @nocollapse */
NzSubmenuService.ctorParameters = () => [
    { type: NzSubmenuService, decorators: [{ type: SkipSelf }, { type: Optional }] },
    { type: NzMenuService }
];
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc3VibWVudS5zZXJ2aWNlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbIm1lbnUvbnotc3VibWVudS5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsVUFBVSxFQUFFLFFBQVEsRUFBRSxRQUFRLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFDL0QsT0FBTyxFQUFFLGFBQWEsRUFBRSxlQUFlLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQy9ELE9BQU8sRUFBRSxTQUFTLEVBQUUsb0JBQW9CLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBRTNFLE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUdsRCxNQUFNLE9BQU8sZ0JBQWdCOzs7OztJQW1EM0IsWUFDa0Msb0JBQXNDLEVBQy9ELGFBQTRCO1FBREgseUJBQW9CLEdBQXBCLG9CQUFvQixDQUFrQjtRQUMvRCxrQkFBYSxHQUFiLGFBQWEsQ0FBZTtRQXBEckMsYUFBUSxHQUFHLEtBQUssQ0FBQztRQUNqQixTQUFJLEdBQXVCLFVBQVUsQ0FBQztRQUN0QyxVQUFLLEdBQUcsSUFBSSxDQUFDLGFBQWEsQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUNuQyxHQUFHOzs7O1FBQUMsSUFBSSxDQUFDLEVBQUU7WUFDVCxJQUFJLElBQUksS0FBSyxRQUFRLEVBQUU7Z0JBQ3JCLE9BQU8sUUFBUSxDQUFDO2FBQ2pCO2lCQUFNLElBQUksSUFBSSxLQUFLLFVBQVUsSUFBSSxJQUFJLENBQUMsb0JBQW9CLEVBQUU7Z0JBQzNELE9BQU8sVUFBVSxDQUFDO2FBQ25CO2lCQUFNO2dCQUNMLE9BQU8sWUFBWSxDQUFDO2FBQ3JCO1FBQ0gsQ0FBQyxFQUFDLEVBQ0YsR0FBRzs7OztRQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsQ0FBQyxJQUFJLENBQUMsSUFBSSxHQUFHLG1CQUFBLElBQUksRUFBc0IsQ0FBQyxFQUFDLENBQ3RELENBQUM7UUFDRixVQUFLLEdBQUcsQ0FBQyxDQUFDO1FBQ1YsV0FBTSxHQUFHLElBQUksZUFBZSxDQUFTLENBQUMsQ0FBQyxDQUFDO1FBQ3hDLGlCQUFZLEdBQUcsSUFBSSxlQUFlLENBQVUsS0FBSyxDQUFDLENBQUM7UUFDbkQsVUFBSyxHQUFHLElBQUksZUFBZSxDQUFVLEtBQUssQ0FBQyxDQUFDO1FBQzVDLHFCQUFnQixHQUFHLElBQUksT0FBTyxFQUFXLENBQUM7UUFDMUMsY0FBUyxHQUFHLGFBQWEsQ0FBQyxJQUFJLENBQUMsWUFBWSxFQUFFLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxDQUFDLElBQUksQ0FDdEUsR0FBRzs7OztRQUFDLEtBQUssQ0FBQyxFQUFFLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxJQUFJLEtBQUssQ0FBQyxDQUFDLENBQUMsRUFBQyxFQUNsQyxTQUFTLENBQUMsR0FBRyxDQUFDLEVBQ2Qsb0JBQW9CLEVBQUUsRUFDdEIsR0FBRzs7OztRQUFDLElBQUksQ0FBQyxFQUFFO1lBQ1QsSUFBSSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUN4QixJQUFJLElBQUksQ0FBQyxvQkFBb0IsRUFBRTtnQkFDN0IsSUFBSSxDQUFDLG9CQUFvQixDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUM7YUFDbkQ7UUFDSCxDQUFDLEVBQUMsQ0FDSCxDQUFDO1FBeUJBLElBQUksSUFBSSxDQUFDLG9CQUFvQixFQUFFO1lBQzdCLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLG9CQUFvQixDQUFDLEtBQUssR0FBRyxDQUFDLENBQUMsQ0FBQztTQUNwRDtJQUNILENBQUM7Ozs7O0lBMUJELFlBQVksQ0FBQyxLQUFjO1FBQ3pCLElBQUksQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDO0lBQ3pCLENBQUM7Ozs7SUFFRCxlQUFlO1FBQ2IsSUFBSSxDQUFDLGtCQUFrQixDQUFDLEtBQUssQ0FBQyxDQUFDO0lBQ2pDLENBQUM7Ozs7O0lBRUQsUUFBUSxDQUFDLEtBQWE7UUFDcEIsSUFBSSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUM7UUFDeEIsSUFBSSxDQUFDLEtBQUssR0FBRyxLQUFLLENBQUM7SUFDckIsQ0FBQzs7Ozs7SUFFRCxrQkFBa0IsQ0FBQyxLQUFjO1FBQy9CLElBQUksQ0FBQyxJQUFJLENBQUMsSUFBSSxLQUFLLFlBQVksSUFBSSxJQUFJLENBQUMsSUFBSSxLQUFLLFVBQVUsSUFBSSxJQUFJLENBQUMsYUFBYSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsRUFBRTtZQUNqSCxJQUFJLENBQUMsZ0JBQWdCLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDO1NBQ25DO0lBQ0gsQ0FBQzs7O1lBbERGLFVBQVU7Ozs7WUFxRCtDLGdCQUFnQix1QkFBckUsUUFBUSxZQUFJLFFBQVE7WUF2RGhCLGFBQWE7Ozs7SUFJcEIsb0NBQWlCOztJQUNqQixnQ0FBc0M7O0lBQ3RDLGlDQVdFOztJQUNGLGlDQUFVOztJQUNWLGtDQUF3Qzs7SUFDeEMsd0NBQW1EOztJQUNuRCxpQ0FBNEM7O0lBQzVDLDRDQUEwQzs7SUFDMUMscUNBVUU7Ozs7O0lBc0JBLGdEQUFzRTs7SUFDdEUseUNBQW1DIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgSW5qZWN0YWJsZSwgT3B0aW9uYWwsIFNraXBTZWxmIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBjb21iaW5lTGF0ZXN0LCBCZWhhdmlvclN1YmplY3QsIFN1YmplY3QgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IGF1ZGl0VGltZSwgZGlzdGluY3RVbnRpbENoYW5nZWQsIG1hcCwgdGFwIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgTnpEaXJlY3Rpb25WSElUeXBlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9kaXJlY3Rpb24nO1xuaW1wb3J0IHsgTnpNZW51U2VydmljZSB9IGZyb20gJy4vbnotbWVudS5zZXJ2aWNlJztcblxuQEluamVjdGFibGUoKVxuZXhwb3J0IGNsYXNzIE56U3VibWVudVNlcnZpY2Uge1xuICBkaXNhYmxlZCA9IGZhbHNlO1xuICBtb2RlOiBOekRpcmVjdGlvblZISVR5cGUgPSAndmVydGljYWwnO1xuICBtb2RlJCA9IHRoaXMubnpNZW51U2VydmljZS5tb2RlJC5waXBlKFxuICAgIG1hcChtb2RlID0+IHtcbiAgICAgIGlmIChtb2RlID09PSAnaW5saW5lJykge1xuICAgICAgICByZXR1cm4gJ2lubGluZSc7XG4gICAgICB9IGVsc2UgaWYgKG1vZGUgPT09ICd2ZXJ0aWNhbCcgfHwgdGhpcy5uekhvc3RTdWJtZW51U2VydmljZSkge1xuICAgICAgICByZXR1cm4gJ3ZlcnRpY2FsJztcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHJldHVybiAnaG9yaXpvbnRhbCc7XG4gICAgICB9XG4gICAgfSksXG4gICAgdGFwKG1vZGUgPT4gKHRoaXMubW9kZSA9IG1vZGUgYXMgTnpEaXJlY3Rpb25WSElUeXBlKSlcbiAgKTtcbiAgbGV2ZWwgPSAxO1xuICBsZXZlbCQgPSBuZXcgQmVoYXZpb3JTdWJqZWN0PG51bWJlcj4oMSk7XG4gIHN1Yk1lbnVPcGVuJCA9IG5ldyBCZWhhdmlvclN1YmplY3Q8Ym9vbGVhbj4oZmFsc2UpO1xuICBvcGVuJCA9IG5ldyBCZWhhdmlvclN1YmplY3Q8Ym9vbGVhbj4oZmFsc2UpO1xuICBtb3VzZUVudGVyTGVhdmUkID0gbmV3IFN1YmplY3Q8Ym9vbGVhbj4oKTtcbiAgbWVudU9wZW4kID0gY29tYmluZUxhdGVzdCh0aGlzLnN1Yk1lbnVPcGVuJCwgdGhpcy5tb3VzZUVudGVyTGVhdmUkKS5waXBlKFxuICAgIG1hcCh2YWx1ZSA9PiB2YWx1ZVswXSB8fCB2YWx1ZVsxXSksXG4gICAgYXVkaXRUaW1lKDE1MCksXG4gICAgZGlzdGluY3RVbnRpbENoYW5nZWQoKSxcbiAgICB0YXAoZGF0YSA9PiB7XG4gICAgICB0aGlzLnNldE9wZW5TdGF0ZShkYXRhKTtcbiAgICAgIGlmICh0aGlzLm56SG9zdFN1Ym1lbnVTZXJ2aWNlKSB7XG4gICAgICAgIHRoaXMubnpIb3N0U3VibWVudVNlcnZpY2Uuc3ViTWVudU9wZW4kLm5leHQoZGF0YSk7XG4gICAgICB9XG4gICAgfSlcbiAgKTtcblxuICBzZXRPcGVuU3RhdGUodmFsdWU6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLm9wZW4kLm5leHQodmFsdWUpO1xuICB9XG5cbiAgb25NZW51SXRlbUNsaWNrKCk6IHZvaWQge1xuICAgIHRoaXMuc2V0TW91c2VFbnRlclN0YXRlKGZhbHNlKTtcbiAgfVxuXG4gIHNldExldmVsKHZhbHVlOiBudW1iZXIpOiB2b2lkIHtcbiAgICB0aGlzLmxldmVsJC5uZXh0KHZhbHVlKTtcbiAgICB0aGlzLmxldmVsID0gdmFsdWU7XG4gIH1cblxuICBzZXRNb3VzZUVudGVyU3RhdGUodmFsdWU6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICBpZiAoKHRoaXMubW9kZSA9PT0gJ2hvcml6b250YWwnIHx8IHRoaXMubW9kZSA9PT0gJ3ZlcnRpY2FsJyB8fCB0aGlzLm56TWVudVNlcnZpY2UuaXNJbkRyb3BEb3duKSAmJiAhdGhpcy5kaXNhYmxlZCkge1xuICAgICAgdGhpcy5tb3VzZUVudGVyTGVhdmUkLm5leHQodmFsdWUpO1xuICAgIH1cbiAgfVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIEBTa2lwU2VsZigpIEBPcHRpb25hbCgpIHByaXZhdGUgbnpIb3N0U3VibWVudVNlcnZpY2U6IE56U3VibWVudVNlcnZpY2UsXG4gICAgcHVibGljIG56TWVudVNlcnZpY2U6IE56TWVudVNlcnZpY2VcbiAgKSB7XG4gICAgaWYgKHRoaXMubnpIb3N0U3VibWVudVNlcnZpY2UpIHtcbiAgICAgIHRoaXMuc2V0TGV2ZWwodGhpcy5uekhvc3RTdWJtZW51U2VydmljZS5sZXZlbCArIDEpO1xuICAgIH1cbiAgfVxufVxuIl19