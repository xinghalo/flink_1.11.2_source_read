/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ComponentFactoryResolver, Directive, ElementRef, Host, Optional, Renderer2, ViewContainerRef } from '@angular/core';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { NzTooltipDirective } from '../tooltip/nz-tooltip.directive';
import { NzPopoverComponent } from './nz-popover.component';
var NzPopoverDirective = /** @class */ (function (_super) {
    tslib_1.__extends(NzPopoverDirective, _super);
    function NzPopoverDirective(elementRef, hostView, resolver, renderer, tooltip, noAnimation) {
        var _this = _super.call(this, elementRef, hostView, resolver, renderer, tooltip, noAnimation) || this;
        _this.noAnimation = noAnimation;
        _this.factory = _this.resolver.resolveComponentFactory(NzPopoverComponent);
        return _this;
    }
    NzPopoverDirective.decorators = [
        { type: Directive, args: [{
                    selector: '[nz-popover]',
                    host: {
                        '[class.ant-popover-open]': 'isTooltipOpen'
                    }
                },] }
    ];
    /** @nocollapse */
    NzPopoverDirective.ctorParameters = function () { return [
        { type: ElementRef },
        { type: ViewContainerRef },
        { type: ComponentFactoryResolver },
        { type: Renderer2 },
        { type: NzPopoverComponent, decorators: [{ type: Optional }] },
        { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
    ]; };
    return NzPopoverDirective;
}(NzTooltipDirective));
export { NzPopoverDirective };
if (false) {
    /** @type {?} */
    NzPopoverDirective.prototype.factory;
    /** @type {?} */
    NzPopoverDirective.prototype.noAnimation;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcG9wb3Zlci5kaXJlY3RpdmUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsicG9wb3Zlci9uei1wb3BvdmVyLmRpcmVjdGl2ZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFFTCx3QkFBd0IsRUFDeEIsU0FBUyxFQUNULFVBQVUsRUFDVixJQUFJLEVBQ0osUUFBUSxFQUNSLFNBQVMsRUFDVCxnQkFBZ0IsRUFDakIsTUFBTSxlQUFlLENBQUM7QUFFdkIsT0FBTyxFQUFFLHNCQUFzQixFQUFFLE1BQU0sZ0RBQWdELENBQUM7QUFDeEYsT0FBTyxFQUFFLGtCQUFrQixFQUFFLE1BQU0saUNBQWlDLENBQUM7QUFDckUsT0FBTyxFQUFFLGtCQUFrQixFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFFNUQ7SUFNd0MsOENBQWtCO0lBR3hELDRCQUNFLFVBQXNCLEVBQ3RCLFFBQTBCLEVBQzFCLFFBQWtDLEVBQ2xDLFFBQW1CLEVBQ1AsT0FBMkIsRUFDWixXQUFvQztRQU5qRSxZQVFFLGtCQUFNLFVBQVUsRUFBRSxRQUFRLEVBQUUsUUFBUSxFQUFFLFFBQVEsRUFBRSxPQUFPLEVBQUUsV0FBVyxDQUFDLFNBQ3RFO1FBSDRCLGlCQUFXLEdBQVgsV0FBVyxDQUF5QjtRQVJqRSxhQUFPLEdBQXlDLEtBQUksQ0FBQyxRQUFRLENBQUMsdUJBQXVCLENBQUMsa0JBQWtCLENBQUMsQ0FBQzs7SUFXMUcsQ0FBQzs7Z0JBbEJGLFNBQVMsU0FBQztvQkFDVCxRQUFRLEVBQUUsY0FBYztvQkFDeEIsSUFBSSxFQUFFO3dCQUNKLDBCQUEwQixFQUFFLGVBQWU7cUJBQzVDO2lCQUNGOzs7O2dCQWhCQyxVQUFVO2dCQUlWLGdCQUFnQjtnQkFOaEIsd0JBQXdCO2dCQUt4QixTQUFTO2dCQU1GLGtCQUFrQix1QkFnQnRCLFFBQVE7Z0JBbEJKLHNCQUFzQix1QkFtQjFCLElBQUksWUFBSSxRQUFROztJQUlyQix5QkFBQztDQUFBLEFBbkJELENBTXdDLGtCQUFrQixHQWF6RDtTQWJZLGtCQUFrQjs7O0lBQzdCLHFDQUEwRzs7SUFReEcseUNBQStEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQ29tcG9uZW50RmFjdG9yeSxcbiAgQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyLFxuICBEaXJlY3RpdmUsXG4gIEVsZW1lbnRSZWYsXG4gIEhvc3QsXG4gIE9wdGlvbmFsLFxuICBSZW5kZXJlcjIsXG4gIFZpZXdDb250YWluZXJSZWZcbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IE56Tm9BbmltYXRpb25EaXJlY3RpdmUgfSBmcm9tICcuLi9jb3JlL25vLWFuaW1hdGlvbi9uei1uby1hbmltYXRpb24uZGlyZWN0aXZlJztcbmltcG9ydCB7IE56VG9vbHRpcERpcmVjdGl2ZSB9IGZyb20gJy4uL3Rvb2x0aXAvbnotdG9vbHRpcC5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgTnpQb3BvdmVyQ29tcG9uZW50IH0gZnJvbSAnLi9uei1wb3BvdmVyLmNvbXBvbmVudCc7XG5cbkBEaXJlY3RpdmUoe1xuICBzZWxlY3RvcjogJ1tuei1wb3BvdmVyXScsXG4gIGhvc3Q6IHtcbiAgICAnW2NsYXNzLmFudC1wb3BvdmVyLW9wZW5dJzogJ2lzVG9vbHRpcE9wZW4nXG4gIH1cbn0pXG5leHBvcnQgY2xhc3MgTnpQb3BvdmVyRGlyZWN0aXZlIGV4dGVuZHMgTnpUb29sdGlwRGlyZWN0aXZlIHtcbiAgZmFjdG9yeTogQ29tcG9uZW50RmFjdG9yeTxOelBvcG92ZXJDb21wb25lbnQ+ID0gdGhpcy5yZXNvbHZlci5yZXNvbHZlQ29tcG9uZW50RmFjdG9yeShOelBvcG92ZXJDb21wb25lbnQpO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsXG4gICAgaG9zdFZpZXc6IFZpZXdDb250YWluZXJSZWYsXG4gICAgcmVzb2x2ZXI6IENvbXBvbmVudEZhY3RvcnlSZXNvbHZlcixcbiAgICByZW5kZXJlcjogUmVuZGVyZXIyLFxuICAgIEBPcHRpb25hbCgpIHRvb2x0aXA6IE56UG9wb3ZlckNvbXBvbmVudCxcbiAgICBASG9zdCgpIEBPcHRpb25hbCgpIHB1YmxpYyBub0FuaW1hdGlvbj86IE56Tm9BbmltYXRpb25EaXJlY3RpdmVcbiAgKSB7XG4gICAgc3VwZXIoZWxlbWVudFJlZiwgaG9zdFZpZXcsIHJlc29sdmVyLCByZW5kZXJlciwgdG9vbHRpcCwgbm9BbmltYXRpb24pO1xuICB9XG59XG4iXX0=