/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ContentChildren, Input, QueryList, TemplateRef, ViewEncapsulation } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { toBoolean } from '../core/util/convert';
import { NzStepComponent } from './nz-step.component';
export class NzStepsComponent {
    constructor() {
        this.nzCurrent = 0;
        this.nzDirection = 'horizontal';
        this.nzLabelPlacement = 'horizontal';
        this.nzSize = 'default';
        this.nzStartIndex = 0;
        this.nzStatus = 'process';
        this.showProcessDot = false;
        this.destroy$ = new Subject();
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzProgressDot(value) {
        if (value instanceof TemplateRef) {
            this.showProcessDot = true;
            this.customProcessDotTemplate = value;
        }
        else {
            this.showProcessDot = toBoolean(value);
        }
        this.updateChildrenSteps();
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzStartIndex || changes.nzDirection || changes.nzStatus || changes.nzCurrent) {
            this.updateChildrenSteps();
        }
        if (changes.nzDirection || changes.nzProgressDot || changes.nzLabelPlacement || changes.nzSize) {
            this.setClassMap();
        }
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.setClassMap();
        this.updateChildrenSteps();
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.destroy$.next();
        this.destroy$.complete();
    }
    /**
     * @return {?}
     */
    ngAfterContentInit() {
        this.updateChildrenSteps();
        if (this.steps) {
            this.steps.changes.pipe(takeUntil(this.destroy$)).subscribe(this.updateChildrenSteps);
        }
    }
    /**
     * @private
     * @return {?}
     */
    updateChildrenSteps() {
        if (this.steps) {
            /** @type {?} */
            const length = this.steps.length;
            this.steps.toArray().forEach((/**
             * @param {?} step
             * @param {?} index
             * @return {?}
             */
            (step, index) => {
                Promise.resolve().then((/**
                 * @return {?}
                 */
                () => {
                    step.outStatus = this.nzStatus;
                    step.showProcessDot = this.showProcessDot;
                    if (this.customProcessDotTemplate) {
                        step.customProcessTemplate = this.customProcessDotTemplate;
                    }
                    step.direction = this.nzDirection;
                    step.index = index + this.nzStartIndex;
                    step.currentIndex = this.nzCurrent;
                    step.last = length === index + 1;
                    step.markForCheck();
                }));
            }));
        }
    }
    /**
     * @private
     * @return {?}
     */
    setClassMap() {
        this.classMap = {
            [`ant-steps-${this.nzDirection}`]: true,
            [`ant-steps-label-horizontal`]: this.nzDirection === 'horizontal',
            [`ant-steps-label-vertical`]: (this.showProcessDot || this.nzLabelPlacement === 'vertical') && this.nzDirection === 'horizontal',
            [`ant-steps-dot`]: this.showProcessDot,
            ['ant-steps-small']: this.nzSize === 'small'
        };
    }
}
NzStepsComponent.decorators = [
    { type: Component, args: [{
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                preserveWhitespaces: false,
                selector: 'nz-steps',
                template: "<div class=\"ant-steps\" [ngClass]=\"classMap\">\n  <ng-content></ng-content>\n</div>"
            }] }
];
NzStepsComponent.propDecorators = {
    steps: [{ type: ContentChildren, args: [NzStepComponent,] }],
    nzCurrent: [{ type: Input }],
    nzDirection: [{ type: Input }],
    nzLabelPlacement: [{ type: Input }],
    nzSize: [{ type: Input }],
    nzStartIndex: [{ type: Input }],
    nzStatus: [{ type: Input }],
    nzProgressDot: [{ type: Input }]
};
if (false) {
    /** @type {?} */
    NzStepsComponent.prototype.steps;
    /** @type {?} */
    NzStepsComponent.prototype.nzCurrent;
    /** @type {?} */
    NzStepsComponent.prototype.nzDirection;
    /** @type {?} */
    NzStepsComponent.prototype.nzLabelPlacement;
    /** @type {?} */
    NzStepsComponent.prototype.nzSize;
    /** @type {?} */
    NzStepsComponent.prototype.nzStartIndex;
    /** @type {?} */
    NzStepsComponent.prototype.nzStatus;
    /** @type {?} */
    NzStepsComponent.prototype.showProcessDot;
    /** @type {?} */
    NzStepsComponent.prototype.customProcessDotTemplate;
    /** @type {?} */
    NzStepsComponent.prototype.classMap;
    /**
     * @type {?}
     * @private
     */
    NzStepsComponent.prototype.destroy$;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc3RlcHMuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInN0ZXBzL256LXN0ZXBzLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUVMLHVCQUF1QixFQUN2QixTQUFTLEVBQ1QsZUFBZSxFQUNmLEtBQUssRUFJTCxTQUFTLEVBRVQsV0FBVyxFQUNYLGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQy9CLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUkzQyxPQUFPLEVBQUUsU0FBUyxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFFakQsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLHFCQUFxQixDQUFDO0FBWXRELE1BQU0sT0FBTyxnQkFBZ0I7SUFQN0I7UUFVVyxjQUFTLEdBQUcsQ0FBQyxDQUFDO1FBQ2QsZ0JBQVcsR0FBb0IsWUFBWSxDQUFDO1FBQzVDLHFCQUFnQixHQUE4QixZQUFZLENBQUM7UUFDM0QsV0FBTSxHQUFpQixTQUFTLENBQUM7UUFDakMsaUJBQVksR0FBRyxDQUFDLENBQUM7UUFDakIsYUFBUSxHQUFpQixTQUFTLENBQUM7UUFZNUMsbUJBQWMsR0FBRyxLQUFLLENBQUM7UUFLZixhQUFRLEdBQUcsSUFBSSxPQUFPLEVBQVEsQ0FBQztJQTBEekMsQ0FBQzs7Ozs7SUF6RUMsSUFDSSxhQUFhLENBQUMsS0FBNkY7UUFDN0csSUFBSSxLQUFLLFlBQVksV0FBVyxFQUFFO1lBQ2hDLElBQUksQ0FBQyxjQUFjLEdBQUcsSUFBSSxDQUFDO1lBQzNCLElBQUksQ0FBQyx3QkFBd0IsR0FBRyxLQUFLLENBQUM7U0FDdkM7YUFBTTtZQUNMLElBQUksQ0FBQyxjQUFjLEdBQUcsU0FBUyxDQUFDLEtBQUssQ0FBQyxDQUFDO1NBQ3hDO1FBQ0QsSUFB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TztTQUM3QyxDQUFDO0lBQ0osQ0FBQzs7O1lBekZGLFNBQVMsU0FBQztnQkFDVCxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtnQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7Z0JBQ3JDLG1CQUFtQixFQUFFLEtBQUs7Z0JBQzFCLFFBQVEsRUFBRSxVQUFVO2dCQUNwQixpR0FBd0M7YUFDekM7OztvQkFFRSxlQUFlLFNBQUMsZUFBZTt3QkFFL0IsS0FBSzswQkFDTCxLQUFLOytCQUNMLEtBQUs7cUJBQ0wsS0FBSzsyQkFDTCxLQUFLO3VCQUNMLEtBQUs7NEJBRUwsS0FBSzs7OztJQVROLGlDQUFvRTs7SUFFcEUscUNBQXVCOztJQUN2Qix1Q0FBcUQ7O0lBQ3JELDRDQUFvRTs7SUFDcEUsa0NBQTBDOztJQUMxQyx3Q0FBMEI7O0lBQzFCLG9DQUE0Qzs7SUFZNUMsMENBQXVCOztJQUN2QixvREFBdUc7O0lBRXZHLG9DQUFzQjs7Ozs7SUFFdEIsb0NBQXVDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQWZ0ZXJDb250ZW50SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENvbXBvbmVudCxcbiAgQ29udGVudENoaWxkcmVuLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgUXVlcnlMaXN0LFxuICBTaW1wbGVDaGFuZ2VzLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBTdWJqZWN0IH0gZnJvbSAncnhqcyc7XG5pbXBvcnQgeyB0YWtlVW50aWwgfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5cbmltcG9ydCB7IE5nQ2xhc3NUeXBlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9uZy1jbGFzcyc7XG5pbXBvcnQgeyBOelNpemVEU1R5cGUgfSBmcm9tICcuLi9jb3JlL3R5cGVzL3NpemUnO1xuaW1wb3J0IHsgdG9Cb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuXG5pbXBvcnQgeyBOelN0ZXBDb21wb25lbnQgfSBmcm9tICcuL256LXN0ZXAuY29tcG9uZW50JztcblxuZXhwb3J0IHR5cGUgTnpEaXJlY3Rpb25UeXBlID0gJ2hvcml6b250YWwnIHwgJ3ZlcnRpY2FsJztcbmV4cG9ydCB0eXBlIE56U3RhdHVzVHlwZSA9ICd3YWl0JyB8ICdwcm9jZXNzJyB8ICdmaW5pc2gnIHwgJ2Vycm9yJztcblxuQENvbXBvbmVudCh7XG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgc2VsZWN0b3I6ICduei1zdGVwcycsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1zdGVwcy5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpTdGVwc0NvbXBvbmVudCBpbXBsZW1lbnRzIE9uQ2hhbmdlcywgT25Jbml0LCBPbkRlc3Ryb3ksIEFmdGVyQ29udGVudEluaXQge1xuICBAQ29udGVudENoaWxkcmVuKE56U3RlcENvbXBvbmVudCkgc3RlcHM6IFF1ZXJ5TGlzdDxOelN0ZXBDb21wb25lbnQ+O1xuXG4gIEBJbnB1dCgpIG56Q3VycmVudCA9IDA7XG4gIEBJbnB1dCgpIG56RGlyZWN0aW9uOiBOekRpcmVjdGlvblR5cGUgPSAnaG9yaXpvbnRhbCc7XG4gIEBJbnB1dCgpIG56TGFiZWxQbGFjZW1lbnQ6ICdob3Jpem9udGFsJyB8ICd2ZXJ0aWNhbCcgPSAnaG9yaXpvbnRhbCc7XG4gIEBJbnB1dCgpIG56U2l6ZTogTnpTaXplRFNUeXBlID0gJ2RlZmF1bHQnO1xuICBASW5wdXQoKSBuelN0YXJ0SW5kZXggPSAwO1xuICBASW5wdXQoKSBuelN0YXR1czogTnpTdGF0dXNUeXBlID0gJ3Byb2Nlc3MnO1xuXG4gIEBJbnB1dCgpXG4gIHNldCBuelByb2dyZXNzRG90KHZhbHVlOiBib29sZWFuIHwgVGVtcGxhdGVSZWY8eyAkaW1wbGljaXQ6IFRlbXBsYXRlUmVmPHZvaWQ+OyBzdGF0dXM6IHN0cmluZzsgaW5kZXg6IG51bWJlciB9Pikge1xuICAgIGlmICh2YWx1ZSBpbnN0YW5jZW9mIFRlbXBsYXRlUmVmKSB7XG4gICAgICB0aGlzLnNob3dQcm9jZXNzRG90ID0gdHJ1ZTtcbiAgICAgIHRoaXMuY3VzdG9tUHJvY2Vzc0RvdFRlbXBsYXRlID0gdmFsdWU7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuc2hvd1Byb2Nlc3NEb3QgPSB0b0Jvb2xlYW4odmFsdWUpO1xuICAgIH1cbiAgICB0aGlzLnVwZGF0ZUNoaWxkcmVuU3RlcHMoKTtcbiAgfVxuICBzaG93UHJvY2Vzc0RvdCA9IGZhbHNlO1xuICBjdXN0b21Qcm9jZXNzRG90VGVtcGxhdGU6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBUZW1wbGF0ZVJlZjx2b2lkPjsgc3RhdHVzOiBzdHJpbmc7IGluZGV4OiBudW1iZXIgfT47XG5cbiAgY2xhc3NNYXA6IE5nQ2xhc3NUeXBlO1xuXG4gIHByaXZhdGUgZGVzdHJveSQgPSBuZXcgU3ViamVjdDx2b2lkPigpO1xuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5uelN0YXJ0SW5kZXggfHwgY2hhbmdlcy5uekRpcmVjdGlvbiB8fCBjaGFuZ2VzLm56U3RhdHVzIHx8IGNoYW5nZXMubnpDdXJyZW50KSB7XG4gICAgICB0aGlzLnVwZGF0ZUNoaWxkcmVuU3RlcHMoKTtcbiAgICB9XG4gICAgaWYgKGNoYW5nZXMubnpEaXJlY3Rpb24gfHwgY2hhbmdlcy5uelByb2dyZXNzRG90IHx8IGNoYW5nZXMubnpMYWJlbFBsYWNlbWVudCB8fCBjaGFuZ2VzLm56U2l6ZSkge1xuICAgICAgdGhpcy5zZXRDbGFzc01hcCgpO1xuICAgIH1cbiAgfVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHRoaXMuc2V0Q2xhc3NNYXAoKTtcbiAgICB0aGlzLnVwZGF0ZUNoaWxkcmVuU3RlcHMoKTtcbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuZGVzdHJveSQubmV4dCgpO1xuICAgIHRoaXMuZGVzdHJveSQuY29tcGxldGUoKTtcbiAgfVxuXG4gIG5nQWZ0ZXJDb250ZW50SW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLnVwZGF0ZUNoaWxkcmVuU3RlcHMoKTtcbiAgICBpZiAodGhpcy5zdGVwcykge1xuICAgICAgdGhpcy5zdGVwcy5jaGFuZ2VzLnBpcGUodGFrZVVudGlsKHRoaXMuZGVzdHJveSQpKS5zdWJzY3JpYmUodGhpcy51cGRhdGVDaGlsZHJlblN0ZXBzKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHVwZGF0ZUNoaWxkcmVuU3RlcHMoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuc3RlcHMpIHtcbiAgICAgIGNvbnN0IGxlbmd0aCA9IHRoaXMuc3RlcHMubGVuZ3RoO1xuICAgICAgdGhpcy5zdGVwcy50b0FycmF5KCkuZm9yRWFjaCgoc3RlcCwgaW5kZXgpID0+IHtcbiAgICAgICAgUHJvbWlzZS5yZXNvbHZlKCkudGhlbigoKSA9PiB7XG4gICAgICAgICAgc3RlcC5vdXRTdGF0dXMgPSB0aGlzLm56U3RhdHVzO1xuICAgICAgICAgIHN0ZXAuc2hvd1Byb2Nlc3NEb3QgPSB0aGlzLnNob3dQcm9jZXNzRG90O1xuICAgICAgICAgIGlmICh0aGlzLmN1c3RvbVByb2Nlc3NEb3RUZW1wbGF0ZSkge1xuICAgICAgICAgICAgc3RlcC5jdXN0b21Qcm9jZXNzVGVtcGxhdGUgPSB0aGlzLmN1c3RvbVByb2Nlc3NEb3RUZW1wbGF0ZTtcbiAgICAgICAgICB9XG4gICAgICAgICAgc3RlcC5kaXJlY3Rpb24gPSB0aGlzLm56RGlyZWN0aW9uO1xuICAgICAgICAgIHN0ZXAuaW5kZXggPSBpbmRleCArIHRoaXMubnpTdGFydEluZGV4O1xuICAgICAgICAgIHN0ZXAuY3VycmVudEluZGV4ID0gdGhpcy5uekN1cnJlbnQ7XG4gICAgICAgICAgc3RlcC5sYXN0ID0gbGVuZ3RoID09PSBpbmRleCArIDE7XG4gICAgICAgICAgc3RlcC5tYXJrRm9yQ2hlY2soKTtcbiAgICAgICAgfSk7XG4gICAgICB9KTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHNldENsYXNzTWFwKCk6IHZvaWQge1xuICAgIHRoaXMuY2xhc3NNYXAgPSB7XG4gICAgICBbYGFudC1zdGVwcy0ke3RoaXMubnpEaXJlY3Rpb259YF06IHRydWUsXG4gICAgICBbYGFudC1zdGVwcy1sYWJlbC1ob3Jpem9udGFsYF06IHRoaXMubnpEaXJlY3Rpb24gPT09ICdob3Jpem9udGFsJyxcbiAgICAgIFtgYW50LXN0ZXBzLWxhYmVsLXZlcnRpY2FsYF06XG4gICAgICAgICh0aGlzLnNob3dQcm9jZXNzRG90IHx8IHRoaXMubnpMYWJlbFBsYWNlbWVudCA9PT0gJ3ZlcnRpY2FsJykgJiYgdGhpcy5uekRpcmVjdGlvbiA9PT0gJ2hvcml6b250YWwnLFxuICAgICAgW2BhbnQtc3RlcHMtZG90YF06IHRoaXMuc2hvd1Byb2Nlc3NEb3QsXG4gICAgICBbJ2FudC1zdGVwcy1zbWFsbCddOiB0aGlzLm56U2l6ZSA9PT0gJ3NtYWxsJ1xuICAgIH07XG4gIH1cbn1cbiJdfQ==