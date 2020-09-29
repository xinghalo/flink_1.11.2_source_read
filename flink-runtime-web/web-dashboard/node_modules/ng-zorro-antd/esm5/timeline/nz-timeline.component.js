/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChild, ContentChildren, ElementRef, Input, QueryList, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { reverseChildNodes } from '../core/util/dom';
import { NzTimelineItemComponent } from './nz-timeline-item.component';
var NzTimelineComponent = /** @class */ (function () {
    function NzTimelineComponent(cdr) {
        this.cdr = cdr;
        this.nzReverse = false;
        this.isPendingBoolean = false;
        this.destroy$ = new Subject();
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    NzTimelineComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        /** @type {?} */
        var modeChanges = changes.nzMode;
        /** @type {?} */
        var reverseChanges = changes.nzReverse;
        /** @type {?} */
        var pendingChanges = changes.nzPending;
        if (modeChanges && (modeChanges.previousValue !== modeChanges.currentValue || modeChanges.isFirstChange())) {
            this.updateChildren();
        }
        if (reverseChanges &&
            reverseChanges.previousValue !== reverseChanges.currentValue &&
            !reverseChanges.isFirstChange()) {
            this.reverseChildTimelineDots();
        }
        if (pendingChanges) {
            this.isPendingBoolean = pendingChanges.currentValue === true;
        }
    };
    /**
     * @return {?}
     */
    NzTimelineComponent.prototype.ngAfterContentInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        this.updateChildren();
        if (this.listOfTimeLine) {
            this.listOfTimeLine.changes.pipe(takeUntil(this.destroy$)).subscribe((/**
             * @return {?}
             */
            function () {
                _this.updateChildren();
            }));
        }
    };
    /**
     * @return {?}
     */
    NzTimelineComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.destroy$.next();
        this.destroy$.complete();
    };
    /**
     * @private
     * @return {?}
     */
    NzTimelineComponent.prototype.updateChildren = /**
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        if (this.listOfTimeLine && this.listOfTimeLine.length) {
            /** @type {?} */
            var length_1 = this.listOfTimeLine.length;
            this.listOfTimeLine.toArray().forEach((/**
             * @param {?} item
             * @param {?} index
             * @return {?}
             */
            function (item, index) {
                item.isLast = !_this.nzReverse ? index === length_1 - 1 : index === 0;
                item.position =
                    _this.nzMode === 'left' || !_this.nzMode
                        ? undefined
                        : _this.nzMode === 'right'
                            ? 'right'
                            : _this.nzMode === 'alternate' && index % 2 === 0
                                ? 'left'
                                : 'right';
                item.detectChanges();
            }));
            this.cdr.markForCheck();
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzTimelineComponent.prototype.reverseChildTimelineDots = /**
     * @private
     * @return {?}
     */
    function () {
        reverseChildNodes((/** @type {?} */ (this.timeline.nativeElement)));
        this.updateChildren();
    };
    NzTimelineComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    preserveWhitespaces: false,
                    selector: 'nz-timeline',
                    template: "<ul\n  class=\"ant-timeline\"\n  [class.ant-timeline-right]=\"nzMode === 'right'\"\n  [class.ant-timeline-alternate]=\"nzMode === 'alternate'\"\n  [class.ant-timeline-pending]=\"!!nzPending\"\n  [class.ant-timeline-reverse]=\"nzReverse\"\n  #timeline>\n  <!-- User inserted timeline dots. -->\n  <ng-content></ng-content>\n  <!-- Pending dot. -->\n  <li *ngIf=\"nzPending\" class=\"ant-timeline-item ant-timeline-item-pending\">\n    <div class=\"ant-timeline-item-tail\"></div>\n    <div class=\"ant-timeline-item-head ant-timeline-item-head-custom ant-timeline-item-head-blue\">\n      <ng-container *nzStringTemplateOutlet=\"nzPendingDot\">\n        {{ nzPendingDot }}<i *ngIf=\"!nzPendingDot\" nz-icon type=\"loading\"></i>\n      </ng-container>\n    </div>\n    <div class=\"ant-timeline-item-content\">\n      <ng-container *nzStringTemplateOutlet=\"nzPending\">\n        {{ isPendingBoolean ? '' : nzPending }}\n      </ng-container>\n    </div>\n  </li>\n</ul>\n"
                }] }
    ];
    /** @nocollapse */
    NzTimelineComponent.ctorParameters = function () { return [
        { type: ChangeDetectorRef }
    ]; };
    NzTimelineComponent.propDecorators = {
        timeline: [{ type: ViewChild, args: ['timeline',] }],
        listOfTimeLine: [{ type: ContentChildren, args: [NzTimelineItemComponent,] }],
        _pendingContent: [{ type: ContentChild, args: ['pending',] }],
        nzMode: [{ type: Input }],
        nzPending: [{ type: Input }],
        nzPendingDot: [{ type: Input }],
        nzReverse: [{ type: Input }]
    };
    return NzTimelineComponent;
}());
export { NzTimelineComponent };
if (false) {
    /** @type {?} */
    NzTimelineComponent.prototype.timeline;
    /** @type {?} */
    NzTimelineComponent.prototype.listOfTimeLine;
    /** @type {?} */
    NzTimelineComponent.prototype._pendingContent;
    /** @type {?} */
    NzTimelineComponent.prototype.nzMode;
    /** @type {?} */
    NzTimelineComponent.prototype.nzPending;
    /** @type {?} */
    NzTimelineComponent.prototype.nzPendingDot;
    /** @type {?} */
    NzTimelineComponent.prototype.nzReverse;
    /** @type {?} */
    NzTimelineComponent.prototype.isPendingBoolean;
    /**
     * @type {?}
     * @private
     */
    NzTimelineComponent.prototype.destroy$;
    /**
     * @type {?}
     * @private
     */
    NzTimelineComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGltZWxpbmUuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRpbWVsaW5lL256LXRpbWVsaW5lLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUVMLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFlBQVksRUFDWixlQUFlLEVBQ2YsVUFBVSxFQUNWLEtBQUssRUFHTCxTQUFTLEVBRVQsV0FBVyxFQUNYLFNBQVMsRUFDVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUFFLE9BQU8sRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUMvQixPQUFPLEVBQUUsU0FBUyxFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFFM0MsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sa0JBQWtCLENBQUM7QUFDckQsT0FBTyxFQUFFLHVCQUF1QixFQUFFLE1BQU0sOEJBQThCLENBQUM7QUFJdkU7SUFxQkUsNkJBQW9CLEdBQXNCO1FBQXRCLFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBTmpDLGNBQVMsR0FBWSxLQUFLLENBQUM7UUFFcEMscUJBQWdCLEdBQVksS0FBSyxDQUFDO1FBRTFCLGFBQVEsR0FBRyxJQUFJLE9BQU8sRUFBUSxDQUFDO0lBRU0sQ0FBQzs7Ozs7SUFFOUMseUNBQVc7Ozs7SUFBWCxVQUFZLE9BQXNCOztZQUMxQixXQUFXLEdBQUcsT0FBTyxDQUFDLE1BQU07O1lBQzVCLGNBQWMsR0FBRyxPQUFPLENBQUMsU0FBUzs7WUFDbEMsY0FBYyxHQUFHLE9BQU8sQ0FBQyxTQUFTO1FBRXhDLElBQUksV0FBVyxJQUFJLENBQUMsV0FBVyxDQUFDLGFBQWEsS0FBSyxXQUFXLENBQUMsWUFBWSxJQUFJLFdBQVcsQ0FBQyxhQUFhLEVBQUUsQ0FBQyxFQUFFO1lBQzFHLElBQUksQ0FBQyxjQUFjLEVBQUUsQ0FBQztTQUN2QjtRQUNE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RCw2Q0FBNkY7O0lBQzdGLDhDQUE0RDs7SUFFNUQscUNBQWdDOztJQUNoQyx3Q0FBeUQ7O0lBQ3pELDJDQUFrRDs7SUFDbEQsd0NBQW9DOztJQUVwQywrQ0FBa0M7Ozs7O0lBRWxDLHVDQUF1Qzs7Ozs7SUFFM0Isa0NBQThCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQWZ0ZXJDb250ZW50SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIENvbnRlbnRDaGlsZCxcbiAgQ29udGVudENoaWxkcmVuLFxuICBFbGVtZW50UmVmLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIFF1ZXJ5TGlzdCxcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBTdWJqZWN0IH0gZnJvbSAncnhqcyc7XG5pbXBvcnQgeyB0YWtlVW50aWwgfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5cbmltcG9ydCB7IHJldmVyc2VDaGlsZE5vZGVzIH0gZnJvbSAnLi4vY29yZS91dGlsL2RvbSc7XG5pbXBvcnQgeyBOelRpbWVsaW5lSXRlbUNvbXBvbmVudCB9IGZyb20gJy4vbnotdGltZWxpbmUtaXRlbS5jb21wb25lbnQnO1xuXG5leHBvcnQgdHlwZSBOelRpbWVsaW5lTW9kZSA9ICdsZWZ0JyB8ICdhbHRlcm5hdGUnIHwgJ3JpZ2h0JztcblxuQENvbXBvbmVudCh7XG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgc2VsZWN0b3I6ICduei10aW1lbGluZScsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei10aW1lbGluZS5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpUaW1lbGluZUNvbXBvbmVudCBpbXBsZW1lbnRzIEFmdGVyQ29udGVudEluaXQsIE9uQ2hhbmdlcywgT25EZXN0cm95IHtcbiAgQFZpZXdDaGlsZCgndGltZWxpbmUnKSB0aW1lbGluZTogRWxlbWVudFJlZjxIVE1MRWxlbWVudD47XG4gIEBDb250ZW50Q2hpbGRyZW4oTnpUaW1lbGluZUl0ZW1Db21wb25lbnQpIGxpc3RPZlRpbWVMaW5lOiBRdWVyeUxpc3Q8TnpUaW1lbGluZUl0ZW1Db21wb25lbnQ+O1xuICBAQ29udGVudENoaWxkKCdwZW5kaW5nJykgX3BlbmRpbmdDb250ZW50OiBUZW1wbGF0ZVJlZjx2b2lkPjtcblxuICBASW5wdXQoKSBuek1vZGU6IE56VGltZWxpbmVNb2RlO1xuICBASW5wdXQoKSBuelBlbmRpbmc6IHN0cmluZyB8IGJvb2xlYW4gfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpQZW5kaW5nRG90OiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpSZXZlcnNlOiBib29sZWFuID0gZmFsc2U7XG5cbiAgaXNQZW5kaW5nQm9vbGVhbjogYm9vbGVhbiA9IGZhbHNlO1xuXG4gIHByaXZhdGUgZGVzdHJveSQgPSBuZXcgU3ViamVjdDx2b2lkPigpO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZikge31cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgY29uc3QgbW9kZUNoYW5nZXMgPSBjaGFuZ2VzLm56TW9kZTtcbiAgICBjb25zdCByZXZlcnNlQ2hhbmdlcyA9IGNoYW5nZXMubnpSZXZlcnNlO1xuICAgIGNvbnN0IHBlbmRpbmdDaGFuZ2VzID0gY2hhbmdlcy5uelBlbmRpbmc7XG5cbiAgICBpZiAobW9kZUNoYW5nZXMgJiYgKG1vZGVDaGFuZ2VzLnByZXZpb3VzVmFsdWUgIT09IG1vZGVDaGFuZ2VzLmN1cnJlbnRWYWx1ZSB8fCBtb2RlQ2hhbmdlcy5pc0ZpcnN0Q2hhbmdlKCkpKSB7XG4gICAgICB0aGlzLnVwZGF0ZUNoaWxkcmVuKCk7XG4gICAgfVxuICAgIGlmIChcbiAgICAgIHJldmVyc2VDaGFuZ2VzICYmXG4gICAgICByZXZlcnNlQ2hhbmdlcy5wcmV2aW91c1ZhbHVlICE9PSByZXZlcnNlQ2hhbmdlcy5jdXJyZW50VmFsdWUgJiZcbiAgICAgICFyZXZlcnNlQ2hhbmdlcy5pc0ZpcnN0Q2hhbmdlKClcbiAgICApIHtcbiAgICAgIHRoaXMucmV2ZXJzZUNoaWxkVGltZWxpbmVEb3RzKCk7XG4gICAgfVxuICAgIGlmIChwZW5kaW5nQ2hhbmdlcykge1xuICAgICAgdGhpcy5pc1BlbmRpbmdCb29sZWFuID0gcGVuZGluZ0NoYW5nZXMuY3VycmVudFZhbHVlID09PSB0cnVlO1xuICAgIH1cbiAgfVxuXG4gIG5nQWZ0ZXJDb250ZW50SW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLnVwZGF0ZUNoaWxkcmVuKCk7XG4gICAgaWYgKHRoaXMubGlzdE9mVGltZUxpbmUpIHtcbiAgICAgIHRoaXMubGlzdE9mVGltZUxpbmUuY2hhbmdlcy5waXBlKHRha2VVbnRpbCh0aGlzLmRlc3Ryb3kkKSkuc3Vic2NyaWJlKCgpID0+IHtcbiAgICAgICAgdGhpcy51cGRhdGVDaGlsZHJlbigpO1xuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5kZXN0cm95JC5uZXh0KCk7XG4gICAgdGhpcy5kZXN0cm95JC5jb21wbGV0ZSgpO1xuICB9XG5cbiAgcHJpdmF0ZSB1cGRhdGVDaGlsZHJlbigpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5saXN0T2ZUaW1lTGluZSAmJiB0aGlzLmxpc3RPZlRpbWVMaW5lLmxlbmd0aCkge1xuICAgICAgY29uc3QgbGVuZ3RoID0gdGhpcy5saXN0T2ZUaW1lTGluZS5sZW5ndGg7XG4gICAgICB0aGlzLmxpc3RPZlRpbWVMaW5lLnRvQXJyYXkoKS5mb3JFYWNoKChpdGVtLCBpbmRleCkgPT4ge1xuICAgICAgICBpdGVtLmlzTGFzdCA9ICF0aGlzLm56UmV2ZXJzZSA/IGluZGV4ID09PSBsZW5ndGggLSAxIDogaW5kZXggPT09IDA7XG4gICAgICAgIGl0ZW0ucG9zaXRpb24gPVxuICAgICAgICAgIHRoaXMubnpNb2RlID09PSAnbGVmdCcgfHwgIXRoaXMubnpNb2RlXG4gICAgICAgICAgICA/IHVuZGVmaW5lZFxuICAgICAgICAgICAgOiB0aGlzLm56TW9kZSA9PT0gJ3JpZ2h0J1xuICAgICAgICAgICAgPyAncmlnaHQnXG4gICAgICAgICAgICA6IHRoaXMubnpNb2RlID09PSAnYWx0ZXJuYXRlJyAmJiBpbmRleCAlIDIgPT09IDBcbiAgICAgICAgICAgID8gJ2xlZnQnXG4gICAgICAgICAgICA6ICdyaWdodCc7XG4gICAgICAgIGl0ZW0uZGV0ZWN0Q2hhbmdlcygpO1xuICAgICAgfSk7XG4gICAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHJldmVyc2VDaGlsZFRpbWVsaW5lRG90cygpOiB2b2lkIHtcbiAgICByZXZlcnNlQ2hpbGROb2Rlcyh0aGlzLnRpbWVsaW5lLm5hdGl2ZUVsZW1lbnQgYXMgSFRNTEVsZW1lbnQpO1xuICAgIHRoaXMudXBkYXRlQ2hpbGRyZW4oKTtcbiAgfVxufVxuIl19