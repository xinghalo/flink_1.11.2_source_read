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
var NzTheadComponent = /** @class */ (function () {
    // tslint:disable-next-line:no-any
    function NzTheadComponent(nzTableComponent, elementRef, renderer) {
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
    NzTheadComponent.prototype.ngAfterContentInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        this.listOfNzThComponent.changes
            .pipe(startWith(true), flatMap((/**
         * @return {?}
         */
        function () { return merge.apply(void 0, tslib_1.__spread(_this.listOfNzThComponent.map((/**
         * @param {?} th
         * @return {?}
         */
        function (th) { return th.nzSortChangeWithKey; })))); })), takeUntil(this.destroy$))
            .subscribe((/**
         * @param {?} data
         * @return {?}
         */
        function (data) {
            _this.nzSortChange.emit(data);
            if (_this.nzSingleSort) {
                _this.listOfNzThComponent.forEach((/**
                 * @param {?} th
                 * @return {?}
                 */
                function (th) {
                    th.nzSort = th.nzSortKey === data.key ? th.nzSort : null;
                    th.marForCheck();
                }));
            }
        }));
    };
    /**
     * @return {?}
     */
    NzTheadComponent.prototype.ngAfterViewInit = /**
     * @return {?}
     */
    function () {
        if (this.nzTableComponent) {
            this.renderer.removeChild(this.renderer.parentNode(this.elementRef.nativeElement), this.elementRef.nativeElement);
        }
    };
    /**
     * @return {?}
     */
    NzTheadComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.destroy$.next();
        this.destroy$.complete();
    };
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
    NzTheadComponent.ctorParameters = function () { return [
        { type: NzTableComponent, decorators: [{ type: Host }, { type: Optional }] },
        { type: ElementRef },
        { type: Renderer2 }
    ]; };
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
    return NzTheadComponent;
}());
export { NzTheadComponent };
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGhlYWQuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRhYmxlL256LXRoZWFkLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFHTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULGVBQWUsRUFDZixVQUFVLEVBQ1YsWUFBWSxFQUNaLElBQUksRUFDSixLQUFLLEVBRUwsUUFBUSxFQUNSLE1BQU0sRUFDTixTQUFTLEVBQ1QsU0FBUyxFQUNULFdBQVcsRUFDWCxTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBRXZCLE9BQU8sRUFBRSxLQUFLLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQ3RDLE9BQU8sRUFBRSxPQUFPLEVBQUUsU0FBUyxFQUFFLFNBQVMsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQy9ELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUNwRCxPQUFPLEVBQUUsZ0JBQWdCLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUN4RCxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sbUJBQW1CLENBQUM7QUFFbEQ7SUFjRSxrQ0FBa0M7SUFDbEMsMEJBQzZCLGdCQUFrQyxFQUNyRCxVQUFzQixFQUN0QixRQUFtQjtRQUZBLHFCQUFnQixHQUFoQixnQkFBZ0IsQ0FBa0I7UUFDckQsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUN0QixhQUFRLEdBQVIsUUFBUSxDQUFXO1FBVnJCLGFBQVEsR0FBRyxJQUFJLE9BQU8sRUFBUSxDQUFDO1FBR2QsaUJBQVksR0FBRyxLQUFLLENBQUM7UUFDM0IsaUJBQVksR0FBRyxJQUFJLFlBQVksRUFBa0MsQ0FBQztRQVFuRixJQUFJLElBQUksQ0FBQyxnQkFBZ0IsRUFBRTtZQUN6QixJQUFJLENBQUMsZ0JBQWdCLENBQUMsZ0JBQWdCLEdBQUcsSUFBSSxDQUFDO1NBQy9DO0lBQ0gsQ0FBQzs7OztJQUVELDZDQUFrQjs7O0lBQWxCO1FBQUEsaUJBZ0JDO1FBZkMsSUFBSSxDQUFDLG1CQUFtQixDQUFDLE9BQU87YUFDN0IsSUFBSSxDQUNILFNBQVMsQ0FBQyxJQUFJLENBQUMsRUFDZixPQUFPOzs7UUFBQyxjQUFNLE9BQUEsS0FBSyxnQ0FBSSxLQUFJLENBQUMsbUJBQW1CLENBQUMsR0FBRzs7OztRQUFDLFVBQUEsRUFBRSxJQUFJLE9BQUEsRUFBRSxDQUFDLG1CQUFtQixFQUF0QixDQUFzQixFQUFDLElBQW5FLENBQW9FLEVBQUMsRUFDbkYsU0FBUyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FDekI7YUFDQSxTQUFTOzs7O1FBQUMsVUFBQyxJQUFvQztZQUM5QyxLQUFJLENBQUMsWUFBWSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUM3QixJQUFJLEtBQUksQ0FBQyxZQUFZLEVBQUU7Z0JBQ3JCLEtBQUksQ0FBQyxtQkFBbUIsQ0FBQyxPQUFPOzs7O2dCQUFDLFVBQUEsRUFBRTtvQkFDakMsRUFBRSxDQUFDLE1BQU0sR0FBRyxFQUFFLENBQUMsU0FBUyxLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQztvQkFDekQsRUFBRSxDQUFDLFdBQVcsRUFBRSxDQUFDO2dCQUNuQixDQUFDLEVBQUMsQ0FBQzthQUNKO1FBQ0gsQ0FBQyxFQUFDLENBQUM7SUFDUCxDQUFDOzs7O0lBRUQsMENBQWU7OztJQUFmO1FBQ0UsSUFBSSxJQUFJLENBQUMsZ0JBQWdCLEVBQUU7WUFDekIsSUFBSSxDQUFDLFFBQVEsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLENBQUMsRUFBRSxJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWEsQ0FBQyxDQUFDO1NBQ25IO0lBQ0gsQ0FBQzs7OztJQUVELHNDQUFXOzs7SUFBWDtRQUNFLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxFQUFFLENBQUM7UUFDckIsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLEVBQUUsQ0FBQztJQUMzQixDQUFDOztnQkFwREYsU0FBUyxTQUFDOztvQkFFVCxRQUFRLEVBQUUsNkJBQTZCO29CQUN2QyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtvQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLDBOQUF3QztpQkFDekM7Ozs7Z0JBVFEsZ0JBQWdCLHVCQW1CcEIsSUFBSSxZQUFJLFFBQVE7Z0JBcENuQixVQUFVO2dCQVFWLFNBQVM7Ozs4QkFxQlIsU0FBUyxTQUFDLGlCQUFpQjtzQ0FDM0IsZUFBZSxTQUFDLGFBQWEsRUFBRSxFQUFFLFdBQVcsRUFBRSxJQUFJLEVBQUU7K0JBQ3BELEtBQUs7K0JBQ0wsTUFBTTs7SUFEa0I7UUFBZixZQUFZLEVBQUU7OzBEQUFzQjtJQTBDaEQsdUJBQUM7Q0FBQSxBQXJERCxJQXFEQztTQTlDWSxnQkFBZ0I7Ozs7OztJQUMzQixvQ0FBdUM7O0lBQ3ZDLHVDQUE2RDs7SUFDN0QsK0NBQXFHOztJQUNyRyx3Q0FBOEM7O0lBQzlDLHdDQUFxRjs7SUFJbkYsNENBQTZEOzs7OztJQUM3RCxzQ0FBOEI7Ozs7O0lBQzlCLG9DQUEyQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIEFmdGVyQ29udGVudEluaXQsXG4gIEFmdGVyVmlld0luaXQsXG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDb21wb25lbnQsXG4gIENvbnRlbnRDaGlsZHJlbixcbiAgRWxlbWVudFJlZixcbiAgRXZlbnRFbWl0dGVyLFxuICBIb3N0LFxuICBJbnB1dCxcbiAgT25EZXN0cm95LFxuICBPcHRpb25hbCxcbiAgT3V0cHV0LFxuICBRdWVyeUxpc3QsXG4gIFJlbmRlcmVyMixcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IG1lcmdlLCBTdWJqZWN0IH0gZnJvbSAncnhqcyc7XG5pbXBvcnQgeyBmbGF0TWFwLCBzdGFydFdpdGgsIHRha2VVbnRpbCB9IGZyb20gJ3J4anMvb3BlcmF0b3JzJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcbmltcG9ydCB7IE56VGFibGVDb21wb25lbnQgfSBmcm9tICcuL256LXRhYmxlLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOelRoQ29tcG9uZW50IH0gZnJvbSAnLi9uei10aC5jb21wb25lbnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOmNvbXBvbmVudC1zZWxlY3RvclxuICBzZWxlY3RvcjogJ3RoZWFkOm5vdCguYW50LXRhYmxlLXRoZWFkKScsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotdGhlYWQuY29tcG9uZW50Lmh0bWwnXG59KVxuZXhwb3J0IGNsYXNzIE56VGhlYWRDb21wb25lbnQgaW1wbGVtZW50cyBBZnRlckNvbnRlbnRJbml0LCBPbkRlc3Ryb3ksIEFmdGVyVmlld0luaXQge1xuICBwcml2YXRlIGRlc3Ryb3kkID0gbmV3IFN1YmplY3Q8dm9pZD4oKTtcbiAgQFZpZXdDaGlsZCgnY29udGVudFRlbXBsYXRlJykgdGVtcGxhdGVSZWY6IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBAQ29udGVudENoaWxkcmVuKE56VGhDb21wb25lbnQsIHsgZGVzY2VuZGFudHM6IHRydWUgfSkgbGlzdE9mTnpUaENvbXBvbmVudDogUXVlcnlMaXN0PE56VGhDb21wb25lbnQ+O1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTaW5nbGVTb3J0ID0gZmFsc2U7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuelNvcnRDaGFuZ2UgPSBuZXcgRXZlbnRFbWl0dGVyPHsga2V5OiBzdHJpbmc7IHZhbHVlOiBzdHJpbmcgfT4oKTtcblxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIGNvbnN0cnVjdG9yKFxuICAgIEBIb3N0KCkgQE9wdGlvbmFsKCkgcHVibGljIG56VGFibGVDb21wb25lbnQ6IE56VGFibGVDb21wb25lbnQsXG4gICAgcHJpdmF0ZSBlbGVtZW50UmVmOiBFbGVtZW50UmVmLFxuICAgIHByaXZhdGUgcmVuZGVyZXI6IFJlbmRlcmVyMlxuICApIHtcbiAgICBpZiAodGhpcy5uelRhYmxlQ29tcG9uZW50KSB7XG4gICAgICB0aGlzLm56VGFibGVDb21wb25lbnQubnpUaGVhZENvbXBvbmVudCA9IHRoaXM7XG4gICAgfVxuICB9XG5cbiAgbmdBZnRlckNvbnRlbnRJbml0KCk6IHZvaWQge1xuICAgIHRoaXMubGlzdE9mTnpUaENvbXBvbmVudC5jaGFuZ2VzXG4gICAgICAucGlwZShcbiAgICAgICAgc3RhcnRXaXRoKHRydWUpLFxuICAgICAgICBmbGF0TWFwKCgpID0+IG1lcmdlKC4uLnRoaXMubGlzdE9mTnpUaENvbXBvbmVudC5tYXAodGggPT4gdGgubnpTb3J0Q2hhbmdlV2l0aEtleSkpKSxcbiAgICAgICAgdGFrZVVudGlsKHRoaXMuZGVzdHJveSQpXG4gICAgICApXG4gICAgICAuc3Vic2NyaWJlKChkYXRhOiB7IGtleTogc3RyaW5nOyB2YWx1ZTogc3RyaW5nIH0pID0+IHtcbiAgICAgICAgdGhpcy5uelNvcnRDaGFuZ2UuZW1pdChkYXRhKTtcbiAgICAgICAgaWYgKHRoaXMubnpTaW5nbGVTb3J0KSB7XG4gICAgICAgICAgdGhpcy5saXN0T2ZOelRoQ29tcG9uZW50LmZvckVhY2godGggPT4ge1xuICAgICAgICAgICAgdGgubnpTb3J0ID0gdGgubnpTb3J0S2V5ID09PSBkYXRhLmtleSA/IHRoLm56U29ydCA6IG51bGw7XG4gICAgICAgICAgICB0aC5tYXJGb3JDaGVjaygpO1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgfVxuXG4gIG5nQWZ0ZXJWaWV3SW5pdCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uelRhYmxlQ29tcG9uZW50KSB7XG4gICAgICB0aGlzLnJlbmRlcmVyLnJlbW92ZUNoaWxkKHRoaXMucmVuZGVyZXIucGFyZW50Tm9kZSh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCksIHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50KTtcbiAgICB9XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLmRlc3Ryb3kkLm5leHQoKTtcbiAgICB0aGlzLmRlc3Ryb3kkLmNvbXBsZXRlKCk7XG4gIH1cbn1cbiJdfQ==