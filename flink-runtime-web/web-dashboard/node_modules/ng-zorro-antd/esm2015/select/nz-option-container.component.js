/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, EventEmitter, Input, NgZone, Output, QueryList, TemplateRef, ViewChild, ViewChildren, ViewEncapsulation } from '@angular/core';
import { fromEvent, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { NzOptionLiComponent } from './nz-option-li.component';
import { NzSelectService } from './nz-select.service';
export class NzOptionContainerComponent {
    /**
     * @param {?} nzSelectService
     * @param {?} cdr
     * @param {?} ngZone
     */
    constructor(nzSelectService, cdr, ngZone) {
        this.nzSelectService = nzSelectService;
        this.cdr = cdr;
        this.ngZone = ngZone;
        this.destroy$ = new Subject();
        this.nzScrollToBottom = new EventEmitter();
    }
    /**
     * @param {?} option
     * @return {?}
     */
    scrollIntoViewIfNeeded(option) {
        // delay after open
        setTimeout((/**
         * @return {?}
         */
        () => {
            if (this.listOfNzOptionLiComponent && this.listOfNzOptionLiComponent.length && option) {
                /** @type {?} */
                const targetOption = this.listOfNzOptionLiComponent.find((/**
                 * @param {?} o
                 * @return {?}
                 */
                o => this.nzSelectService.compareWith(o.nzOption.nzValue, option.nzValue)));
                /* tslint:disable:no-any */
                if (targetOption && targetOption.el && ((/** @type {?} */ (targetOption.el))).scrollIntoViewIfNeeded) {
                    ((/** @type {?} */ (targetOption.el))).scrollIntoViewIfNeeded(false);
                }
                /* tslint:enable:no-any */
            }
        }));
    }
    /**
     * @param {?} _index
     * @param {?} option
     * @return {?}
     */
    trackLabel(_index, option) {
        return option.nzLabel;
    }
    // tslint:disable-next-line:no-any
    /**
     * @param {?} _index
     * @param {?} option
     * @return {?}
     */
    trackValue(_index, option) {
        return option.nzValue;
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.nzSelectService.activatedOption$.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @param {?} option
         * @return {?}
         */
        option => {
            this.scrollIntoViewIfNeeded((/** @type {?} */ (option)));
        }));
        this.nzSelectService.check$.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @return {?}
         */
        () => {
            this.cdr.markForCheck();
        }));
        this.ngZone.runOutsideAngular((/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            const ul = this.dropdownUl.nativeElement;
            fromEvent(ul, 'scroll')
                .pipe(takeUntil(this.destroy$))
                .subscribe((/**
             * @param {?} e
             * @return {?}
             */
            e => {
                e.preventDefault();
                e.stopPropagation();
                if (ul && ul.scrollHeight < ul.clientHeight + ul.scrollTop + 10) {
                    this.ngZone.run((/**
                     * @return {?}
                     */
                    () => {
                        this.nzScrollToBottom.emit();
                    }));
                }
            }));
        }));
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.destroy$.next();
        this.destroy$.complete();
    }
}
NzOptionContainerComponent.decorators = [
    { type: Component, args: [{
                selector: '[nz-option-container]',
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                preserveWhitespaces: false,
                template: "<ul #dropdownUl\n  class=\"ant-select-dropdown-menu ant-select-dropdown-menu-root ant-select-dropdown-menu-vertical\"\n  role=\"menu\"\n  tabindex=\"0\">\n  <li *ngIf=\"nzSelectService.isShowNotFound\"\n    nz-select-unselectable\n    class=\"ant-select-dropdown-menu-item ant-select-dropdown-menu-item-disabled\">\n    <nz-embed-empty [nzComponentName]=\"'select'\" [specificContent]=\"nzNotFoundContent\"></nz-embed-empty>\n  </li>\n  <li nz-option-li\n    *ngIf=\"nzSelectService.addedTagOption\"\n    [nzMenuItemSelectedIcon]=\"nzMenuItemSelectedIcon\"\n    [nzOption]=\"nzSelectService.addedTagOption\">\n  </li>\n  <li nz-option-li\n    *ngFor=\"let option of nzSelectService.listOfNzOptionComponent | nzFilterOption : nzSelectService.searchValue : nzSelectService.filterOption : nzSelectService.serverSearch; trackBy: trackValue\"\n    [nzMenuItemSelectedIcon]=\"nzMenuItemSelectedIcon\"\n    [nzOption]=\"option\">\n  </li>\n  <li class=\"ant-select-dropdown-menu-item-group\"\n    *ngFor=\"let group of nzSelectService.listOfNzOptionGroupComponent | nzFilterGroupOption : nzSelectService.searchValue : nzSelectService.filterOption :nzSelectService.serverSearch; trackBy: trackLabel\">\n    <div class=\"ant-select-dropdown-menu-item-group-title\"\n      [attr.title]=\"group.isLabelString ? group.nzLabel : ''\">\n      <ng-container *nzStringTemplateOutlet=\"group.nzLabel\"> {{group.nzLabel}} </ng-container>\n    </div>\n    <ul class=\"ant-select-dropdown-menu-item-group-list\">\n      <li nz-option-li\n        *ngFor=\"let option of group.listOfNzOptionComponent | nzFilterOption : nzSelectService.searchValue : nzSelectService.filterOption :nzSelectService.serverSearch; trackBy: trackValue\"\n        [nzMenuItemSelectedIcon]=\"nzMenuItemSelectedIcon\"\n        [nzOption]=\"option\">\n      </li>\n    </ul>\n  </li>\n  <li nz-option-li\n    *ngFor=\"let option of nzSelectService.listOfTagOption | nzFilterOption : nzSelectService.searchValue : nzSelectService.filterOption : nzSelectService.serverSearch; trackBy: trackValue \"\n    [nzMenuItemSelectedIcon]=\"nzMenuItemSelectedIcon\"\n    [nzOption]=\"option\">\n  </li>\n</ul>\n"
            }] }
];
/** @nocollapse */
NzOptionContainerComponent.ctorParameters = () => [
    { type: NzSelectService },
    { type: ChangeDetectorRef },
    { type: NgZone }
];
NzOptionContainerComponent.propDecorators = {
    listOfNzOptionLiComponent: [{ type: ViewChildren, args: [NzOptionLiComponent,] }],
    dropdownUl: [{ type: ViewChild, args: ['dropdownUl',] }],
    nzNotFoundContent: [{ type: Input }],
    nzMenuItemSelectedIcon: [{ type: Input }],
    nzScrollToBottom: [{ type: Output }]
};
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzOptionContainerComponent.prototype.destroy$;
    /** @type {?} */
    NzOptionContainerComponent.prototype.listOfNzOptionLiComponent;
    /** @type {?} */
    NzOptionContainerComponent.prototype.dropdownUl;
    /** @type {?} */
    NzOptionContainerComponent.prototype.nzNotFoundContent;
    /** @type {?} */
    NzOptionContainerComponent.prototype.nzMenuItemSelectedIcon;
    /** @type {?} */
    NzOptionContainerComponent.prototype.nzScrollToBottom;
    /** @type {?} */
    NzOptionContainerComponent.prototype.nzSelectService;
    /**
     * @type {?}
     * @private
     */
    NzOptionContainerComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzOptionContainerComponent.prototype.ngZone;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotb3B0aW9uLWNvbnRhaW5lci5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsic2VsZWN0L256LW9wdGlvbi1jb250YWluZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLGlCQUFpQixFQUNqQixTQUFTLEVBQ1QsVUFBVSxFQUNWLFlBQVksRUFDWixLQUFLLEVBQ0wsTUFBTSxFQUdOLE1BQU0sRUFDTixTQUFTLEVBQ1QsV0FBVyxFQUNYLFNBQVMsRUFDVCxZQUFZLEVBQ1osaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxTQUFTLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQzFDLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUUzQyxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSwwQkFBMEIsQ0FBQztBQUUvRCxPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0scUJBQXFCLENBQUM7QUFTdEQsTUFBTSxPQUFPLDBCQUEwQjs7Ozs7O0lBaUNyQyxZQUFtQixlQUFnQyxFQUFVLEdBQXNCLEVBQVUsTUFBYztRQUF4RixvQkFBZSxHQUFmLGVBQWUsQ0FBaUI7UUFBVSxRQUFHLEdBQUgsR0FBRyxDQUFtQjtRQUFVLFdBQU0sR0FBTixNQUFNLENBQVE7UUFoQ25HLGFBQVEsR0FBRyxJQUFJLE9BQU8sRUFBRSxDQUFDO1FBS2QscUJBQWdCLEdBQUcsSUFBSSxZQUFZLEVBQVEsQ0FBQztJQTJCK0MsQ0FBQzs7Ozs7SUF6Qi9HLHNCQUFzQixDQUFDLE1BQXlCO1FBQzlDLG1CQUFtQjtRQUNuQixVQUFVOzs7UUFBQyxHQUFHLEVBQUU7WUFDZCxJQUFJLElBQUksQ0FBQyx5QkFBeUIsSUFBSSxJQUFJLENBQUMseUJBQXlCLENBQUMsTUFBTSxJQUFJLE1BQU0sRUFBRTs7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U0FBUyxTQUFDLFlBQVk7Z0NBQ3RCLEtBQUs7cUNBQ0wsS0FBSzsrQkFDTCxNQUFNOzs7Ozs7O0lBTFAsOENBQWlDOztJQUNqQywrREFBNkY7O0lBQzdGLGdEQUFnRDs7SUFDaEQsdURBQW1DOztJQUNuQyw0REFBbUQ7O0lBQ25ELHNEQUErRDs7SUEyQm5ELHFEQUF1Qzs7Ozs7SUFBRSx5Q0FBOEI7Ozs7O0lBQUUsNENBQXNCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSW5wdXQsXG4gIE5nWm9uZSxcbiAgT25EZXN0cm95LFxuICBPbkluaXQsXG4gIE91dHB1dCxcbiAgUXVlcnlMaXN0LFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0NoaWxkLFxuICBWaWV3Q2hpbGRyZW4sXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgZnJvbUV2ZW50LCBTdWJqZWN0IH0gZnJvbSAncnhqcyc7XG5pbXBvcnQgeyB0YWtlVW50aWwgfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5pbXBvcnQgeyBOek9wdGlvbkdyb3VwQ29tcG9uZW50IH0gZnJvbSAnLi9uei1vcHRpb24tZ3JvdXAuY29tcG9uZW50JztcbmltcG9ydCB7IE56T3B0aW9uTGlDb21wb25lbnQgfSBmcm9tICcuL256LW9wdGlvbi1saS5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpPcHRpb25Db21wb25lbnQgfSBmcm9tICcuL256LW9wdGlvbi5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpTZWxlY3RTZXJ2aWNlIH0gZnJvbSAnLi9uei1zZWxlY3Quc2VydmljZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ1tuei1vcHRpb24tY29udGFpbmVyXScsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgdGVtcGxhdGVVcmw6ICcuL256LW9wdGlvbi1jb250YWluZXIuY29tcG9uZW50Lmh0bWwnXG59KVxuZXhwb3J0IGNsYXNzIE56T3B0aW9uQ29udGFpbmVyQ29tcG9uZW50IGltcGxlbWVudHMgT25EZXN0cm95LCBPbkluaXQge1xuICBwcml2YXRlIGRlc3Ryb3kkID0gbmV3IFN1YmplY3QoKTtcbiAgQFZpZXdDaGlsZHJlbihOek9wdGlvbkxpQ29tcG9uZW50KSBsaXN0T2ZOek9wdGlvbkxpQ29tcG9uZW50OiBRdWVyeUxpc3Q8TnpPcHRpb25MaUNvbXBvbmVudD47XG4gIEBWaWV3Q2hpbGQoJ2Ryb3Bkb3duVWwnKSBkcm9wZG93blVsOiBFbGVtZW50UmVmO1xuICBASW5wdXQoKSBuek5vdEZvdW5kQ29udGVudDogc3RyaW5nO1xuICBASW5wdXQoKSBuek1lbnVJdGVtU2VsZWN0ZWRJY29uOiBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56U2Nyb2xsVG9Cb3R0b20gPSBuZXcgRXZlbnRFbWl0dGVyPHZvaWQ+KCk7XG5cbiAgc2Nyb2xsSW50b1ZpZXdJZk5lZWRlZChvcHRpb246IE56T3B0aW9uQ29tcG9uZW50KTogdm9pZCB7XG4gICAgLy8gZGVsYXkgYWZ0ZXIgb3BlblxuICAgIHNldFRpbWVvdXQoKCkgPT4ge1xuICAgICAgaWYgKHRoaXMubGlzdE9mTnpPcHRpb25MaUNvbXBvbmVudCAmJiB0aGlzLmxpc3RPZk56T3B0aW9uTGlDb21wb25lbnQubGVuZ3RoICYmIG9wdGlvbikge1xuICAgICAgICBjb25zdCB0YXJnZXRPcHRpb24gPSB0aGlzLmxpc3RPZk56T3B0aW9uTGlDb21wb25lbnQuZmluZChvID0+XG4gICAgICAgICAgdGhpcy5uelNlbGVjdFNlcnZpY2UuY29tcGFyZVdpdGgoby5uek9wdGlvbi5uelZhbHVlLCBvcHRpb24ubnpWYWx1ZSlcbiAgICAgICAgKTtcbiAgICAgICAgLyogdHNsaW50OmRpc2FibGU6bm8tYW55ICovXG4gICAgICAgIGlmICh0YXJnZXRPcHRpb24gJiYgdGFyZ2V0T3B0aW9uLmVsICYmICh0YXJnZXRPcHRpb24uZWwgYXMgYW55KS5zY3JvbGxJbnRvVmlld0lmTmVlZGVkKSB7XG4gICAgICAgICAgKHRhcmdldE9wdGlvbi5lbCBhcyBhbnkpLnNjcm9sbEludG9WaWV3SWZOZWVkZWQoZmFsc2UpO1xuICAgICAgICB9XG4gICAgICAgIC8qIHRzbGludDplbmFibGU6bm8tYW55ICovXG4gICAgICB9XG4gICAgfSk7XG4gIH1cblxuICB0cmFja0xhYmVsKF9pbmRleDogbnVtYmVyLCBvcHRpb246IE56T3B0aW9uR3JvdXBDb21wb25lbnQpOiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPiB7XG4gICAgcmV0dXJuIG9wdGlvbi5uekxhYmVsO1xuICB9XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICB0cmFja1ZhbHVlKF9pbmRleDogbnVtYmVyLCBvcHRpb246IE56T3B0aW9uQ29tcG9uZW50KTogYW55IHtcbiAgICByZXR1cm4gb3B0aW9uLm56VmFsdWU7XG4gIH1cblxuICBjb25zdHJ1Y3RvcihwdWJsaWMgbnpTZWxlY3RTZXJ2aWNlOiBOelNlbGVjdFNlcnZpY2UsIHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZiwgcHJpdmF0ZSBuZ1pvbmU6IE5nWm9uZSkge31cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLm56U2VsZWN0U2VydmljZS5hY3RpdmF0ZWRPcHRpb24kLnBpcGUodGFrZVVudGlsKHRoaXMuZGVzdHJveSQpKS5zdWJzY3JpYmUob3B0aW9uID0+IHtcbiAgICAgIHRoaXMuc2Nyb2xsSW50b1ZpZXdJZk5lZWRlZChvcHRpb24hKTtcbiAgICB9KTtcbiAgICB0aGlzLm56U2VsZWN0U2VydmljZS5jaGVjayQucGlwZSh0YWtlVW50aWwodGhpcy5kZXN0cm95JCkpLnN1YnNjcmliZSgoKSA9PiB7XG4gICAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgICB9KTtcbiAgICB0aGlzLm5nWm9uZS5ydW5PdXRzaWRlQW5ndWxhcigoKSA9PiB7XG4gICAgICBjb25zdCB1bCA9IHRoaXMuZHJvcGRvd25VbC5uYXRpdmVFbGVtZW50O1xuICAgICAgZnJvbUV2ZW50PE1vdXNlRXZlbnQ+KHVsLCAnc2Nyb2xsJylcbiAgICAgICAgLnBpcGUodGFrZVVudGlsKHRoaXMuZGVzdHJveSQpKVxuICAgICAgICAuc3Vic2NyaWJlKGUgPT4ge1xuICAgICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgICBlLnN0b3BQcm9wYWdhdGlvbigpO1xuICAgICAgICAgIGlmICh1bCAmJiB1bC5zY3JvbGxIZWlnaHQgPCB1bC5jbGllbnRIZWlnaHQgKyB1bC5zY3JvbGxUb3AgKyAxMCkge1xuICAgICAgICAgICAgdGhpcy5uZ1pvbmUucnVuKCgpID0+IHtcbiAgICAgICAgICAgICAgdGhpcy5uelNjcm9sbFRvQm90dG9tLmVtaXQoKTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgfSk7XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLmRlc3Ryb3kkLm5leHQoKTtcbiAgICB0aGlzLmRlc3Ryb3kkLmNvbXBsZXRlKCk7XG4gIH1cbn1cbiJdfQ==