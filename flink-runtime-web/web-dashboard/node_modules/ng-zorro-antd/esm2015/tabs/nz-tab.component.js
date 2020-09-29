/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, ContentChild, ElementRef, EventEmitter, Input, Output, Renderer2, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { Subject } from 'rxjs';
import { InputBoolean } from '../core/util/convert';
import { NzTabDirective } from './nz-tab.directive';
export class NzTabComponent {
    /**
     * @param {?} elementRef
     * @param {?} renderer
     */
    constructor(elementRef, renderer) {
        this.elementRef = elementRef;
        this.renderer = renderer;
        this.position = null;
        this.origin = null;
        this.isActive = false;
        this.stateChanges = new Subject();
        this.nzForceRender = false;
        this.nzDisabled = false;
        this.nzClick = new EventEmitter();
        this.nzSelect = new EventEmitter();
        this.nzDeselect = new EventEmitter();
        this.renderer.addClass(elementRef.nativeElement, 'ant-tabs-tabpane');
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzTitle || changes.nzForceRender || changes.nzDisabled) {
            this.stateChanges.next();
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.stateChanges.complete();
    }
}
NzTabComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-tab',
                preserveWhitespaces: false,
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                template: "<ng-template>\n  <ng-content></ng-content>\n</ng-template>"
            }] }
];
/** @nocollapse */
NzTabComponent.ctorParameters = () => [
    { type: ElementRef },
    { type: Renderer2 }
];
NzTabComponent.propDecorators = {
    content: [{ type: ViewChild, args: [TemplateRef,] }],
    template: [{ type: ContentChild, args: [NzTabDirective, { read: TemplateRef },] }],
    nzTitle: [{ type: Input }],
    nzForceRender: [{ type: Input }],
    nzDisabled: [{ type: Input }],
    nzClick: [{ type: Output }],
    nzSelect: [{ type: Output }],
    nzDeselect: [{ type: Output }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzTabComponent.prototype, "nzForceRender", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzTabComponent.prototype, "nzDisabled", void 0);
if (false) {
    /** @type {?} */
    NzTabComponent.prototype.position;
    /** @type {?} */
    NzTabComponent.prototype.origin;
    /** @type {?} */
    NzTabComponent.prototype.isActive;
    /** @type {?} */
    NzTabComponent.prototype.stateChanges;
    /** @type {?} */
    NzTabComponent.prototype.content;
    /** @type {?} */
    NzTabComponent.prototype.template;
    /** @type {?} */
    NzTabComponent.prototype.nzTitle;
    /** @type {?} */
    NzTabComponent.prototype.nzForceRender;
    /** @type {?} */
    NzTabComponent.prototype.nzDisabled;
    /** @type {?} */
    NzTabComponent.prototype.nzClick;
    /** @type {?} */
    NzTabComponent.prototype.nzSelect;
    /** @type {?} */
    NzTabComponent.prototype.nzDeselect;
    /** @type {?} */
    NzTabComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzTabComponent.prototype.renderer;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGFiLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJ0YWJzL256LXRhYi5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLFNBQVMsRUFDVCxZQUFZLEVBQ1osVUFBVSxFQUNWLFlBQVksRUFDWixLQUFLLEVBR0wsTUFBTSxFQUNOLFNBQVMsRUFFVCxXQUFXLEVBQ1gsU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBRS9CLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUNwRCxPQUFPLEVBQUUsY0FBYyxFQUFFLE1BQU0sb0JBQW9CLENBQUM7QUFTcEQsTUFBTSxPQUFPLGNBQWM7Ozs7O0lBY3pCLFlBQW1CLFVBQXNCLEVBQVUsUUFBbUI7UUFBbkQsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUFVLGFBQVEsR0FBUixRQUFRLENBQVc7UUFidEUsYUFBUSxHQUFrQixJQUFJLENBQUM7UUFDL0IsV0FBTSxHQUFrQixJQUFJLENBQUM7UUFDN0IsYUFBUSxHQUFHLEtBQUssQ0FBQztRQUNSLGlCQUFZLEdBQUcsSUFBSSxPQUFPLEVBQVEsQ0FBQztRQUluQixrQkFBYSxHQUFHLEtBQUssQ0FBQztRQUN0QixlQUFVLEdBQUcsS0FBSyxDQUFDO1FBQ3pCLFlBQU8sR0FBRyxJQUFJLFlBQVksRUFBUSxDQUFDO1FBQ25DLGFBQVEsR0FBRyxJQUFJLFlBQVksRUFBUSxDQUFDO1FBQ3BDLGVBQVUsR0FBRyxJQUFJLFlBQVksRUFBUSxDQUFDO1FBR3ZELElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsa0JBQWtCLENBQUMsQ0FBQztJQUN2RSxDQUFDOzs7OztJQUVELFdBQVcsQ0FBQyxPQUFzQjtRQUNoQyxJQUFJLE9BQU8sQ0FBQyxPQUFPLElBQUksT0FBTyxDQUFDLGFBQWEsSUFBSSxPQUFPLENBQUMsVUFBVSxFQUFFO1lBQ2xFLElBQUksQ0FBQyxZQUFZLENBQUMsSUFBSSxFQUFFLENBQUM7U0FDMUI7SUFDSCxDQUFDOzs7O0lBRUQsV0FBVztRQUNULElBQUksQ0FBQyxZQUFZLENBQUMsUUFBUSxFQUFFLENBQUM7SUFDL0IsQ0FBQzs7O1lBakNGLFNBQVMsU0FBQztnQkFDVCxRQUFRLEVBQUUsUUFBUTtnQkFDbEIsbUJBQW1CLEVBQUUsS0FBSztnQkFDMUIsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7Z0JBQ3JDLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO2dCQUMvQyxzRUFBc0M7YUFDdkM7Ozs7WUF2QkMsVUFBVTtZQU1WLFNBQVM7OztzQkF1QlIsU0FBUyxTQUFDLFdBQVc7dUJBQ3JCLFlBQVksU0FBQyxjQUFjLEVBQUUsRUFBRSxJQUFJLEVBQUUsV0FBVyxFQUFFO3NCQUNsRCxLQUFLOzRCQUNMLEtBQUs7eUJBQ0wsS0FBSztzQkFDTCxNQUFNO3VCQUNOLE1BQU07eUJBQ04sTUFBTTs7QUFKa0I7SUFBZixZQUFZLEVBQUU7O3FEQUF1QjtBQUN0QjtJQUFmLFlBQVksRUFBRTs7a0RBQW9COzs7SUFSNUMsa0NBQStCOztJQUMvQixnQ0FBNkI7O0lBQzdCLGtDQUFpQjs7SUFDakIsc0NBQTRDOztJQUM1QyxpQ0FBbUQ7O0lBQ25ELGtDQUFpRjs7SUFDakYsaUNBQTZDOztJQUM3Qyx1Q0FBK0M7O0lBQy9DLG9DQUE0Qzs7SUFDNUMsaUNBQXNEOztJQUN0RCxrQ0FBdUQ7O0lBQ3ZELG9DQUF5RDs7SUFFN0Msb0NBQTZCOzs7OztJQUFFLGtDQUEyQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDb21wb25lbnQsXG4gIENvbnRlbnRDaGlsZCxcbiAgRWxlbWVudFJlZixcbiAgRXZlbnRFbWl0dGVyLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIE91dHB1dCxcbiAgUmVuZGVyZXIyLFxuICBTaW1wbGVDaGFuZ2VzLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0NoaWxkLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IFN1YmplY3QgfSBmcm9tICdyeGpzJztcblxuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuaW1wb3J0IHsgTnpUYWJEaXJlY3RpdmUgfSBmcm9tICcuL256LXRhYi5kaXJlY3RpdmUnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei10YWInLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei10YWIuY29tcG9uZW50Lmh0bWwnXG59KVxuZXhwb3J0IGNsYXNzIE56VGFiQ29tcG9uZW50IGltcGxlbWVudHMgT25DaGFuZ2VzLCBPbkRlc3Ryb3kge1xuICBwb3NpdGlvbjogbnVtYmVyIHwgbnVsbCA9IG51bGw7XG4gIG9yaWdpbjogbnVtYmVyIHwgbnVsbCA9IG51bGw7XG4gIGlzQWN0aXZlID0gZmFsc2U7XG4gIHJlYWRvbmx5IHN0YXRlQ2hhbmdlcyA9IG5ldyBTdWJqZWN0PHZvaWQ+KCk7XG4gIEBWaWV3Q2hpbGQoVGVtcGxhdGVSZWYpIGNvbnRlbnQ6IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBAQ29udGVudENoaWxkKE56VGFiRGlyZWN0aXZlLCB7IHJlYWQ6IFRlbXBsYXRlUmVmIH0pIHRlbXBsYXRlOiBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpUaXRsZTogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekZvcmNlUmVuZGVyID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekRpc2FibGVkID0gZmFsc2U7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuekNsaWNrID0gbmV3IEV2ZW50RW1pdHRlcjx2b2lkPigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpTZWxlY3QgPSBuZXcgRXZlbnRFbWl0dGVyPHZvaWQ+KCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuekRlc2VsZWN0ID0gbmV3IEV2ZW50RW1pdHRlcjx2b2lkPigpO1xuXG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBlbGVtZW50UmVmOiBFbGVtZW50UmVmLCBwcml2YXRlIHJlbmRlcmVyOiBSZW5kZXJlcjIpIHtcbiAgICB0aGlzLnJlbmRlcmVyLmFkZENsYXNzKGVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2FudC10YWJzLXRhYnBhbmUnKTtcbiAgfVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5uelRpdGxlIHx8IGNoYW5nZXMubnpGb3JjZVJlbmRlciB8fCBjaGFuZ2VzLm56RGlzYWJsZWQpIHtcbiAgICAgIHRoaXMuc3RhdGVDaGFuZ2VzLm5leHQoKTtcbiAgICB9XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLnN0YXRlQ2hhbmdlcy5jb21wbGV0ZSgpO1xuICB9XG59XG4iXX0=