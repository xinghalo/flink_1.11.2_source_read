/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, ContentChildren, ElementRef, EventEmitter, Host, Input, Optional, Output, QueryList, Renderer2, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { merge, Subject } from 'rxjs';
import { flatMap, startWith, takeUntil } from 'rxjs/operators';
import { InputBoolean } from '../core/util/convert';
import { NzTableComponent } from './nz-table.component';
import { NzThComponent } from './nz-th.component';
export class NzTheadComponent {
    // tslint:disable-next-line:no-any
    /**
     * @param {?} nzTableComponent
     * @param {?} elementRef
     * @param {?} renderer
     */
    constructor(nzTableComponent, elementRef, renderer) {
        this.nzTableComponent = nzTableComponent;
        this.elementRef = elementRef;
        this.renderer = renderer;
        this.destroy$ = new Subject();
        this.nzSingleSort = false;
        this.nzSortChange = new EventEmitter();
        if (this.nzTableComponent) {
            this.nzTableComponent.nzTheadComponent = this;
        }
    }
    /**
     * @return {?}
     */
    ngAfterContentInit() {
        this.listOfNzThComponent.changes
            .pipe(startWith(true), flatMap((/**
         * @return {?}
         */
        () => merge(...this.listOfNzThComponent.map((/**
         * @param {?} th
         * @return {?}
         */
        th => th.nzSortChangeWithKey))))), takeUntil(this.destroy$))
            .subscribe((/**
         * @param {?} data
         * @return {?}
         */
        (data) => {
            this.nzSortChange.emit(data);
            if (this.nzSingleSort) {
                this.listOfNzThComponent.forEach((/**
                 * @param {?} th
                 * @return {?}
                 */
                th => {
                    th.nzSort = th.nzSortKey === data.key ? th.nzSort : null;
                    th.marForCheck();
                }));
            }
        }));
    }
    /**
     * @return {?}
     */
    ngAfterViewInit() {
        if (this.nzTableComponent) {
            this.renderer.removeChild(this.renderer.parentNode(this.elementRef.nativeElement), this.elementRef.nativeElement);
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.destroy$.next();
        this.destroy$.complete();
    }
}
NzTheadComponent.decorators = [
    { type: Component, args: [{
                // tslint:disable-next-line:component-selector
                selector: 'thead:not(.ant-table-thead)',
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                template: "<ng-template #contentTemplate>\n  <ng-content></ng-content>\n</ng-template>\n<ng-container *ngIf=\"!nzTableComponent\">\n  <ng-template [ngTemplateOutlet]=\"contentTemplate\"></ng-template>\n</ng-container>"
            }] }
];
/** @nocollapse */
NzTheadComponent.ctorParameters = () => [
    { type: NzTableComponent, decorators: [{ type: Host }, { type: Optional }] },
    { type: ElementRef },
    { type: Renderer2 }
];
NzTheadComponent.propDecorators = {
    templateRef: [{ type: ViewChild, args: ['contentTemplate',] }],
    listOfNzThComponent: [{ type: ContentChildren, args: [NzThComponent, { descendants: true },] }],
    nzSingleSort: [{ type: Input }],
    nzSortChange: [{ type: Output }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzTheadComponent.prototype, "nzSingleSort", void 0);
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzTheadComponent.prototype.destroy$;
    /** @type {?} */
    NzTheadComponent.prototype.templateRef;
    /** @type {?} */
    NzTheadComponent.prototype.listOfNzThComponent;
    /** @type {?} */
    NzTheadComponent.prototype.nzSingleSort;
    /** @type {?} */
    NzTheadComponent.prototype.nzSortChange;
    /** @type {?} */
    NzTheadComponent.prototype.nzTableComponent;
    /**
     * @type {?}
     * @private
     */
    NzTheadComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzTheadComponent.prototype.renderer;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGhlYWQuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRhYmxlL256LXRoZWFkLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFHTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULGVBQWUsRUFDZixVQUFVLEVBQ1YsWUFBWSxFQUNaLElBQUksRUFDSixLQUFLLEVBRUwsUUFBUSxFQUNSLE1BQU0sRUFDTixTQUFTLEVBQ1QsU0FBUyxFQUNULFdBQVcsRUFDWCxTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBRXZCLE9BQU8sRUFBRSxLQUFLLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQ3RDLE9BQU8sRUFBRSxPQUFPLEVBQUUsU0FBUyxFQUFFLFNBQVMsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQy9ELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUNwRCxPQUFPLEVBQUUsZ0JBQWdCLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUN4RCxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sbUJBQW1CLENBQUM7QUFTbEQsTUFBTSxPQUFPLGdCQUFnQjs7Ozs7OztJQVEzQixZQUM2QixnQkFBa0MsRUFDckQsVUFBc0IsRUFDdEIsUUFBbUI7UUFGQSxxQkFBZ0IsR0FBaEIsZ0JBQWdCLENBQWtCO1FBQ3JELGVBQVUsR0FBVixVQUFVLENBQVk7UUFDdEIsYUFBUSxHQUFSLFFBQVEsQ0FBVztRQVZyQixhQUFRLEdBQUcsSUFBSSxPQUFPLEVBQVEsQ0FBQztRQUdkLGlCQUFZLEdBQUcsS0FBSyxDQUFDO1FBQzNCLGlCQUFZLEdBQUcsSUFBSSxZQUFZLEVBQWtDLENBQUM7UUFRbkYsSUFBSSxJQUFJLENBQUMsZ0JBQWdCLEVBQUU7WUFDekIsSUFBSSxDQUFDLGdCQUFnQixDQUFDLGdCQUFnQixHQUFHLElBQUksQ0FBQztTQUMvQztJQUNILENBQUM7Ozs7SUFFRCxrQkFBa0I7UUFDaEIsSUFBSSxDQUFDLG1CQUFtQixDQUFDLE9BQU87YUFDN0IsSUFBSSxDQUNILFNBQVMsQ0FBQyxJQUFJLENBQUMsRUFDZixPQUFPOzs7UUFBQyxHQUFHLEVBQUUsQ0FBQyxLQUFLLENBQUMsR0FBRyxJQUFJLENBQUMsbUJBQW1CLENBQUMsR0FBRzs7OztRQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLG1CQUFtQixFQUFDLENBQUMsRUFBQyxFQUNuRixTQUFTLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxDQUN6QjthQUNBLFNBQVM7Ozs7UUFBQyxDQUFDLElBQW9DLEVBQUUsRUFBRTtZQUNsRCxJQUFJLENBQUMsWUFBWSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUM3QixJQUFJLElBQUksQ0FBQyxZQUFZLEVBQUU7Z0JBQ3JCLElBQUksQ0FBQyxtQkFBbUIsQ0FBQyxPQUFPOzs7O2dCQUFDLEVBQUUsQ0FBQyxFQUFFO29CQUNwQyxFQUFFLENBQUMsTUFBTSxHQUFHLEVBQUUsQ0FBQyxTQUFTLEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO29CQUN6RCxFQUFFLENBQUMsV0FBVyxFQUFFLENBQUM7Z0JBQ25CLENBQUMsRUFBQyxDQUFDO2FBQ0o7UUFDSCxDQUFDLEVBQUMsQ0FBQztJQUNQLENBQUM7Ozs7SUFFRCxlQUFlO1FBQ2IsSUFBSSxJQUFJLENBQUMsZ0JBQWdCLEVBQUU7WUFDekIsSUFBSSxDQUFDLFFBQVEsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLENBQUMsRUFBRSxJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWEsQ0FBQyxDQUFDO1NBQ25IO0lBQ0gsQ0FBQzs7OztJQUVELFdBQVc7UUFDVCxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksRUFBRSxDQUFDO1FBQ3JCLElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxFQUFFLENBQUM7SUFDM0IsQ0FBQzs7O1lBcERGLFNBQVMsU0FBQzs7Z0JBRVQsUUFBUSxFQUFFLDZCQUE2QjtnQkFDdkMsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07Z0JBQy9DLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO2dCQUNyQywwTkFBd0M7YUFDekM7Ozs7WUFUUSxnQkFBZ0IsdUJBbUJwQixJQUFJLFlBQUksUUFBUTtZQXBDbkIsVUFBVTtZQVFWLFNBQVM7OzswQkFxQlIsU0FBUyxTQUFDLGlCQUFpQjtrQ0FDM0IsZUFBZSxTQUFDLGFBQWEsRUFBRSxFQUFFLFdBQVcsRUFBRSxJQUFJLEVBQUU7MkJBQ3BELEtBQUs7MkJBQ0wsTUFBTTs7QUFEa0I7SUFBZixZQUFZLEVBQUU7O3NEQUFzQjs7Ozs7O0lBSDlDLG9DQUF1Qzs7SUFDdkMsdUNBQTZEOztJQUM3RCwrQ0FBcUc7O0lBQ3JHLHdDQUE4Qzs7SUFDOUMsd0NBQXFGOztJQUluRiw0Q0FBNkQ7Ozs7O0lBQzdELHNDQUE4Qjs7Ozs7SUFDOUIsb0NBQTJCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQWZ0ZXJDb250ZW50SW5pdCxcbiAgQWZ0ZXJWaWV3SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENvbXBvbmVudCxcbiAgQ29udGVudENoaWxkcmVuLFxuICBFbGVtZW50UmVmLFxuICBFdmVudEVtaXR0ZXIsXG4gIEhvc3QsXG4gIElucHV0LFxuICBPbkRlc3Ryb3ksXG4gIE9wdGlvbmFsLFxuICBPdXRwdXQsXG4gIFF1ZXJ5TGlzdCxcbiAgUmVuZGVyZXIyLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0NoaWxkLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgbWVyZ2UsIFN1YmplY3QgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IGZsYXRNYXAsIHN0YXJ0V2l0aCwgdGFrZVVudGlsIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuaW1wb3J0IHsgTnpUYWJsZUNvbXBvbmVudCB9IGZyb20gJy4vbnotdGFibGUuY29tcG9uZW50JztcbmltcG9ydCB7IE56VGhDb21wb25lbnQgfSBmcm9tICcuL256LXRoLmNvbXBvbmVudCc7XG5cbkBDb21wb25lbnQoe1xuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6Y29tcG9uZW50LXNlbGVjdG9yXG4gIHNlbGVjdG9yOiAndGhlYWQ6bm90KC5hbnQtdGFibGUtdGhlYWQpJyxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei10aGVhZC5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpUaGVhZENvbXBvbmVudCBpbXBsZW1lbnRzIEFmdGVyQ29udGVudEluaXQsIE9uRGVzdHJveSwgQWZ0ZXJWaWV3SW5pdCB7XG4gIHByaXZhdGUgZGVzdHJveSQgPSBuZXcgU3ViamVjdDx2b2lkPigpO1xuICBAVmlld0NoaWxkKCdjb250ZW50VGVtcGxhdGUnKSB0ZW1wbGF0ZVJlZjogVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBDb250ZW50Q2hpbGRyZW4oTnpUaENvbXBvbmVudCwgeyBkZXNjZW5kYW50czogdHJ1ZSB9KSBsaXN0T2ZOelRoQ29tcG9uZW50OiBRdWVyeUxpc3Q8TnpUaENvbXBvbmVudD47XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelNpbmdsZVNvcnQgPSBmYWxzZTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56U29ydENoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8eyBrZXk6IHN0cmluZzsgdmFsdWU6IHN0cmluZyB9PigpO1xuXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgY29uc3RydWN0b3IoXG4gICAgQEhvc3QoKSBAT3B0aW9uYWwoKSBwdWJsaWMgbnpUYWJsZUNvbXBvbmVudDogTnpUYWJsZUNvbXBvbmVudCxcbiAgICBwcml2YXRlIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsXG4gICAgcHJpdmF0ZSByZW5kZXJlcjogUmVuZGVyZXIyXG4gICkge1xuICAgIGlmICh0aGlzLm56VGFibGVDb21wb25lbnQpIHtcbiAgICAgIHRoaXMubnpUYWJsZUNvbXBvbmVudC5uelRoZWFkQ29tcG9uZW50ID0gdGhpcztcbiAgICB9XG4gIH1cblxuICBuZ0FmdGVyQ29udGVudEluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5saXN0T2ZOelRoQ29tcG9uZW50LmNoYW5nZXNcbiAgICAgIC5waXBlKFxuICAgICAgICBzdGFydFdpdGgodHJ1ZSksXG4gICAgICAgIGZsYXRNYXAoKCkgPT4gbWVyZ2UoLi4udGhpcy5saXN0T2ZOelRoQ29tcG9uZW50Lm1hcCh0aCA9PiB0aC5uelNvcnRDaGFuZ2VXaXRoS2V5KSkpLFxuICAgICAgICB0YWtlVW50aWwodGhpcy5kZXN0cm95JClcbiAgICAgIClcbiAgICAgIC5zdWJzY3JpYmUoKGRhdGE6IHsga2V5OiBzdHJpbmc7IHZhbHVlOiBzdHJpbmcgfSkgPT4ge1xuICAgICAgICB0aGlzLm56U29ydENoYW5nZS5lbWl0KGRhdGEpO1xuICAgICAgICBpZiAodGhpcy5uelNpbmdsZVNvcnQpIHtcbiAgICAgICAgICB0aGlzLmxpc3RPZk56VGhDb21wb25lbnQuZm9yRWFjaCh0aCA9PiB7XG4gICAgICAgICAgICB0aC5uelNvcnQgPSB0aC5uelNvcnRLZXkgPT09IGRhdGEua2V5ID8gdGgubnpTb3J0IDogbnVsbDtcbiAgICAgICAgICAgIHRoLm1hckZvckNoZWNrKCk7XG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICB9XG5cbiAgbmdBZnRlclZpZXdJbml0KCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm56VGFibGVDb21wb25lbnQpIHtcbiAgICAgIHRoaXMucmVuZGVyZXIucmVtb3ZlQ2hpbGQodGhpcy5yZW5kZXJlci5wYXJlbnROb2RlKHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50KSwgdGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQpO1xuICAgIH1cbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuZGVzdHJveSQubmV4dCgpO1xuICAgIHRoaXMuZGVzdHJveSQuY29tcGxldGUoKTtcbiAgfVxufVxuIl19