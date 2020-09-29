/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/** keep track https://github.com/angular/material2/issues/5007 **/
import { ConnectionPositionPair, Overlay, OverlayConfig } from '@angular/cdk/overlay';
import { ComponentPortal } from '@angular/cdk/portal';
import { Injectable } from '@angular/core';
import { fromEvent } from 'rxjs';
import { filter, take } from 'rxjs/operators';
import { NzDropdownContextComponent } from './nz-dropdown-context.component';
import * as i0 from "@angular/core";
import * as i1 from "./public-api";
import * as i2 from "@angular/cdk/overlay";
var NzDropdownService = /** @class */ (function () {
    function NzDropdownService(overlay) {
        this.overlay = overlay;
    }
    /**
     * @param {?} $event
     * @param {?} templateRef
     * @return {?}
     */
    NzDropdownService.prototype.create = /**
     * @param {?} $event
     * @param {?} templateRef
     * @return {?}
     */
    function ($event, templateRef) {
        var _this = this;
        $event.preventDefault();
        this.dispose();
        this.overlayRef = this.overlay.create(new OverlayConfig({
            scrollStrategy: this.overlay.scrollStrategies.close(),
            positionStrategy: this.overlay
                .position()
                .flexibleConnectedTo({
                x: $event.x,
                y: $event.y
            })
                .withPositions([
                new ConnectionPositionPair({ originX: 'start', originY: 'top' }, { overlayX: 'start', overlayY: 'top' }),
                new ConnectionPositionPair({ originX: 'start', originY: 'top' }, { overlayX: 'start', overlayY: 'bottom' }),
                new ConnectionPositionPair({ originX: 'start', originY: 'top' }, { overlayX: 'end', overlayY: 'bottom' }),
                new ConnectionPositionPair({ originX: 'start', originY: 'top' }, { overlayX: 'end', overlayY: 'top' })
            ])
        }));
        /** @type {?} */
        var positionChanges = ((/** @type {?} */ (this.overlayRef.getConfig().positionStrategy)))
            .positionChanges;
        /** @type {?} */
        var instance = this.overlayRef.attach(new ComponentPortal(NzDropdownContextComponent)).instance;
        fromEvent(document, 'click')
            .pipe(filter((/**
         * @param {?} event
         * @return {?}
         */
        function (event) { return !!_this.overlayRef && !_this.overlayRef.overlayElement.contains((/** @type {?} */ (event.target))); })), take(1))
            .subscribe((/**
         * @return {?}
         */
        function () { return instance.close(); }));
        instance.init(true, templateRef, positionChanges, this);
        return instance;
    };
    /**
     * @return {?}
     */
    NzDropdownService.prototype.dispose = /**
     * @return {?}
     */
    function () {
        if (this.overlayRef) {
            this.overlayRef.dispose();
            this.overlayRef = null;
        }
    };
    NzDropdownService.decorators = [
        { type: Injectable, args: [{
                    providedIn: 'root'
                },] }
    ];
    /** @nocollapse */
    NzDropdownService.ctorParameters = function () { return [
        { type: Overlay }
    ]; };
    /** @nocollapse */ NzDropdownService.ngInjectableDef = i0.defineInjectable({ factory: function NzDropdownService_Factory() { return new i1.NzDropdownService(i0.inject(i2.Overlay)); }, token: i1.NzDropdownService, providedIn: "root" });
    return NzDropdownService;
}());
export { NzDropdownService };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzDropdownService.prototype.overlayRef;
    /**
     * @type {?}
     * @private
     */
    NzDropdownService.prototype.overlay;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZHJvcGRvd24uc2VydmljZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkcm9wZG93bi9uei1kcm9wZG93bi5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7O0FBQ0EsT0FBTyxFQUNMLHNCQUFzQixFQUV0QixPQUFPLEVBQ1AsYUFBYSxFQUVkLE1BQU0sc0JBQXNCLENBQUM7QUFDOUIsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLHFCQUFxQixDQUFDO0FBQ3RELE9BQU8sRUFBRSxVQUFVLEVBQWUsTUFBTSxlQUFlLENBQUM7QUFDeEQsT0FBTyxFQUFFLFNBQVMsRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUNqQyxPQUFPLEVBQUUsTUFBTSxFQUFFLElBQUksRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQzlDLE9BQU8sRUFBRSwwQkFBMEIsRUFBRSxNQUFNLGlDQUFpQyxDQUFDOzs7O0FBRTdFO0lBTUUsMkJBQW9CLE9BQWdCO1FBQWhCLFlBQU8sR0FBUCxPQUFPLENBQVM7SUFBRyxDQUFDOzs7Ozs7SUFFeEMsa0NBQU07Ozs7O0lBQU4sVUFBTyxNQUFrQixFQUFFLFdBQThCO1FBQXpELGlCQStCQztRQTlCQyxNQUFNLENBQUMsY0FBYyxFQUFFLENBQUM7UUFDeEIsSUFBSSxDQUFDLE9BQU8sRUFBRSxDQUFDO1FBQ2YsSUFBSSxDQUFDLFVBQVUsR0FBRyxJQUFJLENBQUMsT0FBTyxDQUFDLE1BQU0sQ0FDbkMsSUFBSSxhQUFhLENBQUM7WUFDaEIsY0FBYyxFQUFFLElBQUksQ0FBQyxPQUFPLENBQUMsZ0JBQWdCLENBQUMsS0FBSyxFQUFFO1lBQ3JELGdCQUFnQixFQUFFLElBQUksQ0FBQyxPQUFPO2lCQUMzQixRQUFRLEVBQUU7aUJBQ1YsbUJBQW1CLENBQUM7Z0JBQ25CLENBQUMsRUFBRSxNQUFNLENBQUMsQ0FBQztnQkFDWCxDQUFDLEVBQUUsTUFBTSxDQUFDLENBQUM7YUFDWixDQUFDO2lCQUNELGFBQWEsQ0FBQztnQkFDYixJQUFJLHNCQUFzQixDQUFDLEVBQUUsT0FBTyxFQUFFLE9BQU8sRUFBRSxPQUFPLEVBQUUsS0FBSyxFQUFFLEVBQUUsRUFBRSxRQUFRLEVBQUUsT0FBTyxFQUFFLFFBQVEsRUFBRSxLQUFLLEVBQUUsQ0FBQztnQkFDeEcsSUFBSSxzQkFBc0IsQ0FBQyxFQUFFLE9BQU8sRUFBRSxPQUFPLEVBQUUsT0FBTyxFQUFFLEtBQUssRUFBRSxFQUFFLEVBQUUsUUFBUSxFQUFFLE9BQU8sRUFBRSxRQUFRLEVBQUUsUUFBUSxFQUFFLENBQUM7Z0JBQzNHLElBQUksc0JBQXNCLENBQUMsRUFBRSxPQUFPLEVBQUUsT0FBTyxFQUFFLE9BQU8sRUFBRSxLQUFLLEVBQUUsRUFBRSxFQUFFLFFBQVEsRUFBRSxLQUFLLEVBQUUsUUFBUSxFQUFFLFFBQVEsRUFBRSxDQUFDO2dCQUN6RyxJQUFJLHNCQUFzQixDQUFDLEVBQUUsT0FBTyxFQUFFLE9BQU8sRUFBRSxPQUFPLEVBQUUsS0FBSyxFQUFFLEVBQUUsRUFBRSxRQUFRLEVBQUUsS0FBSyxFQUFFLFFBQVEsRUFBRSxLQUFLLEVBQUUsQ0FBQzthQUN2RyxDQUFDO1NBQ0wsQ0FBQyxDQUNILENBQUM7O1lBQ0ksZUFBZSxHQUFHLENBQUMsbUJBQUEsSUFBSSxDQUFDLFVBQVUsQ0FBQyxTQUFTLEVBQUUsQ0FBQyxnQkFBZ0IsRUFBcUMsQ0FBQzthQUN4RyxlQUFlOztZQUNaLFFBQVEsR0FBRyxJQUFJLENBQUMsVUFBVSxDQUFDLE1BQU0sQ0FBQyxJQUFJLGVBQWUsQ0FBQywwQkFBMEIsQ0FBQyxDQUFDLENBQUMsUUFBUTtRQUNqRyxTQUFTLENBQWEsUUFBUSxFQUFFLE9BQU8sQ0FBQzthQUNyQyxJQUFJLENBQ0gsTUFBTTs7OztRQUFDLFVBQUEsS0FBSyxJQUFJLE9BQUEsQ0FBQyxDQUFDLEtBQUksQ0FBQyxVQUFVLElBQUksQ0FBQyxLQUFJLENBQUMsVUFBVSxDQUFDLGNBQWMsQ0FBQyxRQUFRLENBQUMsbUJBQUEsS0FBSyxDQUFDLE1BQU0sRUFBZSxDQUFDLEVBQTFGLENBQTBGLEVBQUMsRUFDM0csSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUNSO2FBQ0EsU0FBUzs7O1FBQUMsY0FBTSxPQUFBLFFBQVEsQ0FBQyxLQUFLLEVBQUUsRUFBaEIsQ0FBZ0IsRUFBQyxDQUFDO1FBQ3JDLFFBQVEsQ0FBQyxJQUFJLENBQUMsSUFBSSxFQUFFLFdBQVcsRUFBRSxlQUFlLEVBQUUsSUFBSSxDQUFDLENBQUM7UUFDeEQsT0FBTyxRQUFRLENBQUM7SUFDbEIsQ0FBQzs7OztJQUVELG1DQUFPOzs7SUFBUDtRQUNFLElBQUksSUFBSSxDQUFDLFVBQVUsRUFBRTtZQUNuQixJQUFJLENBQUMsVUFBVSxDQUFDLE9BQU8sRUFBRSxDQUFDO1lBQzFCLElBQUksQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDO1NBQ3hCO0lBQ0gsQ0FBQzs7Z0JBOUNGLFVBQVUsU0FBQztvQkFDVixVQUFVLEVBQUUsTUFBTTtpQkFDbkI7Ozs7Z0JBWkMsT0FBTzs7OzRCQUpUO0NBNkRDLEFBL0NELElBK0NDO1NBNUNZLGlCQUFpQjs7Ozs7O0lBQzVCLHVDQUFzQzs7Ozs7SUFFMUIsb0NBQXdCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqIGtlZXAgdHJhY2sgaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvbWF0ZXJpYWwyL2lzc3Vlcy81MDA3ICoqL1xuaW1wb3J0IHtcbiAgQ29ubmVjdGlvblBvc2l0aW9uUGFpcixcbiAgRmxleGlibGVDb25uZWN0ZWRQb3NpdGlvblN0cmF0ZWd5LFxuICBPdmVybGF5LFxuICBPdmVybGF5Q29uZmlnLFxuICBPdmVybGF5UmVmXG59IGZyb20gJ0Bhbmd1bGFyL2Nkay9vdmVybGF5JztcbmltcG9ydCB7IENvbXBvbmVudFBvcnRhbCB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9wb3J0YWwnO1xuaW1wb3J0IHsgSW5qZWN0YWJsZSwgVGVtcGxhdGVSZWYgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IGZyb21FdmVudCB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgZmlsdGVyLCB0YWtlIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgTnpEcm9wZG93bkNvbnRleHRDb21wb25lbnQgfSBmcm9tICcuL256LWRyb3Bkb3duLWNvbnRleHQuY29tcG9uZW50JztcblxuQEluamVjdGFibGUoe1xuICBwcm92aWRlZEluOiAncm9vdCdcbn0pXG5leHBvcnQgY2xhc3MgTnpEcm9wZG93blNlcnZpY2Uge1xuICBwcml2YXRlIG92ZXJsYXlSZWY6IE92ZXJsYXlSZWYgfCBudWxsO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgb3ZlcmxheTogT3ZlcmxheSkge31cblxuICBjcmVhdGUoJGV2ZW50OiBNb3VzZUV2ZW50LCB0ZW1wbGF0ZVJlZjogVGVtcGxhdGVSZWY8dm9pZD4pOiBOekRyb3Bkb3duQ29udGV4dENvbXBvbmVudCB7XG4gICAgJGV2ZW50LnByZXZlbnREZWZhdWx0KCk7XG4gICAgdGhpcy5kaXNwb3NlKCk7XG4gICAgdGhpcy5vdmVybGF5UmVmID0gdGhpcy5vdmVybGF5LmNyZWF0ZShcbiAgICAgIG5ldyBPdmVybGF5Q29uZmlnKHtcbiAgICAgICAgc2Nyb2xsU3RyYXRlZ3k6IHRoaXMub3ZlcmxheS5zY3JvbGxTdHJhdGVnaWVzLmNsb3NlKCksXG4gICAgICAgIHBvc2l0aW9uU3RyYXRlZ3k6IHRoaXMub3ZlcmxheVxuICAgICAgICAgIC5wb3NpdGlvbigpXG4gICAgICAgICAgLmZsZXhpYmxlQ29ubmVjdGVkVG8oe1xuICAgICAgICAgICAgeDogJGV2ZW50LngsXG4gICAgICAgICAgICB5OiAkZXZlbnQueVxuICAgICAgICAgIH0pXG4gICAgICAgICAgLndpdGhQb3NpdGlvbnMoW1xuICAgICAgICAgICAgbmV3IENvbm5lY3Rpb25Qb3NpdGlvblBhaXIoeyBvcmlnaW5YOiAnc3RhcnQnLCBvcmlnaW5ZOiAndG9wJyB9LCB7IG92ZXJsYXlYOiAnc3RhcnQnLCBvdmVybGF5WTogJ3RvcCcgfSksXG4gICAgICAgICAgICBuZXcgQ29ubmVjdGlvblBvc2l0aW9uUGFpcih7IG9yaWdpblg6ICdzdGFydCcsIG9yaWdpblk6ICd0b3AnIH0sIHsgb3ZlcmxheVg6ICdzdGFydCcsIG92ZXJsYXlZOiAnYm90dG9tJyB9KSxcbiAgICAgICAgICAgIG5ldyBDb25uZWN0aW9uUG9zaXRpb25QYWlyKHsgb3JpZ2luWDogJ3N0YXJ0Jywgb3JpZ2luWTogJ3RvcCcgfSwgeyBvdmVybGF5WDogJ2VuZCcsIG92ZXJsYXlZOiAnYm90dG9tJyB9KSxcbiAgICAgICAgICAgIG5ldyBDb25uZWN0aW9uUG9zaXRpb25QYWlyKHsgb3JpZ2luWDogJ3N0YXJ0Jywgb3JpZ2luWTogJ3RvcCcgfSwgeyBvdmVybGF5WDogJ2VuZCcsIG92ZXJsYXlZOiAndG9wJyB9KVxuICAgICAgICAgIF0pXG4gICAgICB9KVxuICAgICk7XG4gICAgY29uc3QgcG9zaXRpb25DaGFuZ2VzID0gKHRoaXMub3ZlcmxheVJlZi5nZXRDb25maWcoKS5wb3NpdGlvblN0cmF0ZWd5IGFzIEZsZXhpYmxlQ29ubmVjdGVkUG9zaXRpb25TdHJhdGVneSlcbiAgICAgIC5wb3NpdGlvbkNoYW5nZXM7XG4gICAgY29uc3QgaW5zdGFuY2UgPSB0aGlzLm92ZXJsYXlSZWYuYXR0YWNoKG5ldyBDb21wb25lbnRQb3J0YWwoTnpEcm9wZG93bkNvbnRleHRDb21wb25lbnQpKS5pbnN0YW5jZTtcbiAgICBmcm9tRXZlbnQ8TW91c2VFdmVudD4oZG9jdW1lbnQsICdjbGljaycpXG4gICAgICAucGlwZShcbiAgICAgICAgZmlsdGVyKGV2ZW50ID0+ICEhdGhpcy5vdmVybGF5UmVmICYmICF0aGlzLm92ZXJsYXlSZWYub3ZlcmxheUVsZW1lbnQuY29udGFpbnMoZXZlbnQudGFyZ2V0IGFzIEhUTUxFbGVtZW50KSksXG4gICAgICAgIHRha2UoMSlcbiAgICAgIClcbiAgICAgIC5zdWJzY3JpYmUoKCkgPT4gaW5zdGFuY2UuY2xvc2UoKSk7XG4gICAgaW5zdGFuY2UuaW5pdCh0cnVlLCB0ZW1wbGF0ZVJlZiwgcG9zaXRpb25DaGFuZ2VzLCB0aGlzKTtcbiAgICByZXR1cm4gaW5zdGFuY2U7XG4gIH1cblxuICBkaXNwb3NlKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm92ZXJsYXlSZWYpIHtcbiAgICAgIHRoaXMub3ZlcmxheVJlZi5kaXNwb3NlKCk7XG4gICAgICB0aGlzLm92ZXJsYXlSZWYgPSBudWxsO1xuICAgIH1cbiAgfVxufVxuIl19