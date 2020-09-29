/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Overlay } from '@angular/cdk/overlay';
import { ComponentPortal } from '@angular/cdk/portal';
import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { NzDrawerComponent } from './nz-drawer.component';
import * as i0 from "@angular/core";
import * as i1 from "./public-api";
import * as i2 from "@angular/cdk/overlay";
/**
 * @template R
 */
export class DrawerBuilderForService {
    /**
     * @param {?} overlay
     * @param {?} options
     */
    constructor(overlay, options) {
        this.overlay = overlay;
        this.options = options;
        this.unsubscribe$ = new Subject();
        this.createDrawer();
        this.updateOptions(this.options);
        // Prevent repeatedly open drawer when tap focus element.
        (/** @type {?} */ (this.drawerRef)).instance.savePreviouslyFocusedElement();
        (/** @type {?} */ (this.drawerRef)).instance.nzOnViewInit.pipe(takeUntil(this.unsubscribe$)).subscribe((/**
         * @return {?}
         */
        () => {
            (/** @type {?} */ (this.drawerRef)).instance.open();
        }));
        (/** @type {?} */ (this.drawerRef)).instance.nzOnClose.subscribe((/**
         * @return {?}
         */
        () => {
            (/** @type {?} */ (this.drawerRef)).instance.close();
        }));
        (/** @type {?} */ (this.drawerRef)).instance.afterClose.pipe(takeUntil(this.unsubscribe$)).subscribe((/**
         * @return {?}
         */
        () => {
            this.overlayRef.dispose();
            this.drawerRef = null;
            this.unsubscribe$.next();
            this.unsubscribe$.complete();
        }));
    }
    /**
     * @return {?}
     */
    getInstance() {
        return (/** @type {?} */ (this.drawerRef)) && (/** @type {?} */ (this.drawerRef)).instance;
    }
    /**
     * @return {?}
     */
    createDrawer() {
        this.overlayRef = this.overlay.create();
        this.drawerRef = this.overlayRef.attach(new ComponentPortal(NzDrawerComponent));
    }
    /**
     * @param {?} options
     * @return {?}
     */
    updateOptions(options) {
        Object.assign((/** @type {?} */ (this.drawerRef)).instance, options);
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    DrawerBuilderForService.prototype.drawerRef;
    /**
     * @type {?}
     * @private
     */
    DrawerBuilderForService.prototype.overlayRef;
    /**
     * @type {?}
     * @private
     */
    DrawerBuilderForService.prototype.unsubscribe$;
    /**
     * @type {?}
     * @private
     */
    DrawerBuilderForService.prototype.overlay;
    /**
     * @type {?}
     * @private
     */
    DrawerBuilderForService.prototype.options;
}
export class NzDrawerService {
    /**
     * @param {?} overlay
     */
    constructor(overlay) {
        this.overlay = overlay;
    }
    // tslint:disable-next-line:no-any
    /**
     * @template T, D, R
     * @param {?} options
     * @return {?}
     */
    create(options) {
        return new DrawerBuilderForService(this.overlay, options).getInstance();
    }
}
NzDrawerService.decorators = [
    { type: Injectable, args: [{ providedIn: 'root' },] }
];
/** @nocollapse */
NzDrawerService.ctorParameters = () => [
    { type: Overlay }
];
/** @nocollapse */ NzDrawerService.ngInjectableDef = i0.defineInjectable({ factory: function NzDrawerService_Factory() { return new i1.NzDrawerService(i0.inject(i2.Overlay)); }, token: i1.NzDrawerService, providedIn: "root" });
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzDrawerService.prototype.overlay;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZHJhd2VyLnNlcnZpY2UuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiZHJhd2VyL256LWRyYXdlci5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsT0FBTyxFQUFjLE1BQU0sc0JBQXNCLENBQUM7QUFDM0QsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLHFCQUFxQixDQUFDO0FBQ3RELE9BQU8sRUFBZ0IsVUFBVSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ3pELE9BQU8sRUFBRSxPQUFPLEVBQUUsTUFBTSxNQUFNLENBQUM7QUFDL0IsT0FBTyxFQUFFLFNBQVMsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBRzNDLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLHVCQUF1QixDQUFDOzs7Ozs7O0FBRTFELE1BQU0sT0FBTyx1QkFBdUI7Ozs7O0lBS2xDLFlBQW9CLE9BQWdCLEVBQVUsT0FBd0I7UUFBbEQsWUFBTyxHQUFQLE9BQU8sQ0FBUztRQUFVLFlBQU8sR0FBUCxPQUFPLENBQWlCO1FBRjlELGlCQUFZLEdBQUcsSUFBSSxPQUFPLEVBQVEsQ0FBQztRQUd6QyxJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7UUFDcEIsSUFBSSxDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUM7UUFDakMseURBQXlEO1FBQ3pELG1CQUFBLElBQUksQ0FBQyxTQUFTLEVBQUMsQ0FBQyxRQUFRLENBQUMsNEJBQTRCLEVBQUUsQ0FBQztRQUN4RCxtQkFBQSxJQUFJLENBQUMsU0FBUyxFQUFDLENBQUMsUUFBUSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxZQUFZLENBQUMsQ0FBQyxDQUFDLFNBQVM7OztRQUFDLEdBQUcsRUFBRTtZQUN0RixtQkFBQSxJQUFJLENBQUMsU0FBUyxFQUFDLENBQUMsUUFBUSxDQUFDLElBQUksRUFBRSxDQUFDO1FBQ2xDLENBQUMsRUFBQyxDQUFDO1FBRUgsbUJBQUEsSUFBSSxDQUFDLFNBQVMsRUFBQyxDQUFDLFFBQVEsQ0FBQyxTQUFTLENBQUMsU0FBUzs7O1FBQUMsR0FBRyxFQUFFO1lBQ2hELG1CQUFBLElBQUksQ0FBQyxTQUFTLEVBQUMsQ0FBQyxRQUFRLENBQUMsS0FBSyxFQUFFLENBQUM7UUFDbkMsQ0FBQyxFQUFDLENBQUM7UUFFSCxtQkFBQSxJQUFJLENBQUMsU0FBUyxFQUFDLENBQUMsUUFBUSxDQUFDLFVBQVUsQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxZQUFZLENBQUMsQ0FBQyxDQUFDLFNBQVM7OztRQUFDLEdBQUcsRUFBRTtZQUNwRixJQUFJLENBQUMsVUFBVSxDQUFDLE9BQU8sRUFBRSxDQUFDO1lBQzFCLElBQUksQ0FBQyxTQUFTLEdBQUcsSUFBSSxDQUFDO1lBQ3RCLElBQUksQ0FBQyxZQUFZLENBQUMsSUFBSSxFQUFFLENBQUM7WUFDekIsSUFBSSxDQUFDLFlBQVksQ0FBQyxRQUFRLEVBQUUsQ0FBQztRQUMvQixDQUFDLEVBQUMsQ0FBQztJQUNMLENBQUM7Ozs7SUFFRCxXQUFXO1FBQ1QsT0FBTyxtQkFBQSxJQUFJLENBQUMsU0FBUyxFQUFDLElBQUksbUJBQUEsSUFBSSxDQUFDLFNBQVMsRUFBQyxDQUFDLFFBQVEsQ0FBQztJQUNyRCxDQUFDOzs7O0lBRUQsWUFBWTtRQUNWLElBQUksQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQyxNQUFNLEVBQUUsQ0FBQztRQUN4QyxJQUFJLENBQUMsU0FBUyxHQUFHLElBQUksQ0FBQyxVQUFVLENBQUMsTUFBTSxDQUFDLElBQUksZUFBZSxDQUFDLGlCQUFpQixDQUFDLENBQUMsQ0FBQztJQUNsRixDQUFDOzs7OztJQUVELGFBQWEsQ0FBQyxPQUF3QjtRQUNwQyxNQUFNLENBQUMsTUFBTSxDQUFDLG1CQUFBLElBQUksQ0FBQyxTQUFTLEVBQUMsQ0FBQyxRQUFRLEVBQUUsT0FBTyxDQUFDLENBQUM7SUFDbkQsQ0FBQztDQUNGOzs7Ozs7SUFyQ0MsNENBQTBEOzs7OztJQUMxRCw2Q0FBK0I7Ozs7O0lBQy9CLCtDQUEyQzs7Ozs7SUFFL0IsMENBQXdCOzs7OztJQUFFLDBDQUFnQzs7QUFvQ3hFLE1BQU0sT0FBTyxlQUFlOzs7O0lBQzFCLFlBQW9CLE9BQWdCO1FBQWhCLFlBQU8sR0FBUCxPQUFPLENBQVM7SUFBRyxDQUFDOzs7Ozs7O0lBR3hDLE1BQU0sQ0FBNEIsT0FBOEI7UUFDOUQsT0FBTyxJQUFJLHVCQUF1QixDQUFJLElBQUksQ0FBQyxPQUFPLEVBQUUsT0FBTyxDQUFDLENBQUMsV0FBVyxFQUFFLENBQUM7SUFDN0UsQ0FBQzs7O1lBUEYsVUFBVSxTQUFDLEVBQUUsVUFBVSxFQUFFLE1BQU0sRUFBRTs7OztZQWpEekIsT0FBTzs7Ozs7Ozs7SUFtREYsa0NBQXdCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgT3ZlcmxheSwgT3ZlcmxheVJlZiB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9vdmVybGF5JztcbmltcG9ydCB7IENvbXBvbmVudFBvcnRhbCB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9wb3J0YWwnO1xuaW1wb3J0IHsgQ29tcG9uZW50UmVmLCBJbmplY3RhYmxlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBTdWJqZWN0IH0gZnJvbSAncnhqcyc7XG5pbXBvcnQgeyB0YWtlVW50aWwgfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5pbXBvcnQgeyBOekRyYXdlck9wdGlvbnMgfSBmcm9tICcuL256LWRyYXdlci1vcHRpb25zJztcbmltcG9ydCB7IE56RHJhd2VyUmVmIH0gZnJvbSAnLi9uei1kcmF3ZXItcmVmJztcbmltcG9ydCB7IE56RHJhd2VyQ29tcG9uZW50IH0gZnJvbSAnLi9uei1kcmF3ZXIuY29tcG9uZW50JztcblxuZXhwb3J0IGNsYXNzIERyYXdlckJ1aWxkZXJGb3JTZXJ2aWNlPFI+IHtcbiAgcHJpdmF0ZSBkcmF3ZXJSZWY6IENvbXBvbmVudFJlZjxOekRyYXdlckNvbXBvbmVudD4gfCBudWxsO1xuICBwcml2YXRlIG92ZXJsYXlSZWY6IE92ZXJsYXlSZWY7XG4gIHByaXZhdGUgdW5zdWJzY3JpYmUkID0gbmV3IFN1YmplY3Q8dm9pZD4oKTtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIG92ZXJsYXk6IE92ZXJsYXksIHByaXZhdGUgb3B0aW9uczogTnpEcmF3ZXJPcHRpb25zKSB7XG4gICAgdGhpcy5jcmVhdGVEcmF3ZXIoKTtcbiAgICB0aGlzLnVwZGF0ZU9wdGlvbnModGhpcy5vcHRpb25zKTtcbiAgICAvLyBQcmV2ZW50IHJlcGVhdGVkbHkgb3BlbiBkcmF3ZXIgd2hlbiB0YXAgZm9jdXMgZWxlbWVudC5cbiAgICB0aGlzLmRyYXdlclJlZiEuaW5zdGFuY2Uuc2F2ZVByZXZpb3VzbHlGb2N1c2VkRWxlbWVudCgpO1xuICAgIHRoaXMuZHJhd2VyUmVmIS5pbnN0YW5jZS5uek9uVmlld0luaXQucGlwZSh0YWtlVW50aWwodGhpcy51bnN1YnNjcmliZSQpKS5zdWJzY3JpYmUoKCkgPT4ge1xuICAgICAgdGhpcy5kcmF3ZXJSZWYhLmluc3RhbmNlLm9wZW4oKTtcbiAgICB9KTtcblxuICAgIHRoaXMuZHJhd2VyUmVmIS5pbnN0YW5jZS5uek9uQ2xvc2Uuc3Vic2NyaWJlKCgpID0+IHtcbiAgICAgIHRoaXMuZHJhd2VyUmVmIS5pbnN0YW5jZS5jbG9zZSgpO1xuICAgIH0pO1xuXG4gICAgdGhpcy5kcmF3ZXJSZWYhLmluc3RhbmNlLmFmdGVyQ2xvc2UucGlwZSh0YWtlVW50aWwodGhpcy51bnN1YnNjcmliZSQpKS5zdWJzY3JpYmUoKCkgPT4ge1xuICAgICAgdGhpcy5vdmVybGF5UmVmLmRpc3Bvc2UoKTtcbiAgICAgIHRoaXMuZHJhd2VyUmVmID0gbnVsbDtcbiAgICAgIHRoaXMudW5zdWJzY3JpYmUkLm5leHQoKTtcbiAgICAgIHRoaXMudW5zdWJzY3JpYmUkLmNvbXBsZXRlKCk7XG4gICAgfSk7XG4gIH1cblxuICBnZXRJbnN0YW5jZSgpOiBOekRyYXdlclJlZjxSPiB7XG4gICAgcmV0dXJuIHRoaXMuZHJhd2VyUmVmISAmJiB0aGlzLmRyYXdlclJlZiEuaW5zdGFuY2U7XG4gIH1cblxuICBjcmVhdGVEcmF3ZXIoKTogdm9pZCB7XG4gICAgdGhpcy5vdmVybGF5UmVmID0gdGhpcy5vdmVybGF5LmNyZWF0ZSgpO1xuICAgIHRoaXMuZHJhd2VyUmVmID0gdGhpcy5vdmVybGF5UmVmLmF0dGFjaChuZXcgQ29tcG9uZW50UG9ydGFsKE56RHJhd2VyQ29tcG9uZW50KSk7XG4gIH1cblxuICB1cGRhdGVPcHRpb25zKG9wdGlvbnM6IE56RHJhd2VyT3B0aW9ucyk6IHZvaWQge1xuICAgIE9iamVjdC5hc3NpZ24odGhpcy5kcmF3ZXJSZWYhLmluc3RhbmNlLCBvcHRpb25zKTtcbiAgfVxufVxuXG5ASW5qZWN0YWJsZSh7IHByb3ZpZGVkSW46ICdyb290JyB9KVxuZXhwb3J0IGNsYXNzIE56RHJhd2VyU2VydmljZSB7XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgb3ZlcmxheTogT3ZlcmxheSkge31cblxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIGNyZWF0ZTxUID0gYW55LCBEID0gYW55LCBSID0gYW55PihvcHRpb25zOiBOekRyYXdlck9wdGlvbnM8VCwgRD4pOiBOekRyYXdlclJlZjxSPiB7XG4gICAgcmV0dXJuIG5ldyBEcmF3ZXJCdWlsZGVyRm9yU2VydmljZTxSPih0aGlzLm92ZXJsYXksIG9wdGlvbnMpLmdldEluc3RhbmNlKCk7XG4gIH1cbn1cbiJdfQ==