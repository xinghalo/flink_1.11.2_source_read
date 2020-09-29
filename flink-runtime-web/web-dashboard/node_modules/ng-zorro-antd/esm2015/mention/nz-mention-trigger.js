/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { forwardRef, Directive, ElementRef, EventEmitter } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
/** @type {?} */
export const NZ_MENTION_TRIGGER_ACCESSOR = {
    provide: NG_VALUE_ACCESSOR,
    useExisting: forwardRef((/**
     * @return {?}
     */
    () => NzMentionTriggerDirective)),
    multi: true
};
export class NzMentionTriggerDirective {
    /**
     * @param {?} el
     */
    constructor(el) {
        this.el = el;
        this.onFocusin = new EventEmitter();
        this.onBlur = new EventEmitter();
        this.onInput = new EventEmitter();
        this.onKeydown = new EventEmitter();
        this.onClick = new EventEmitter();
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.completeEvents();
    }
    /**
     * @return {?}
     */
    completeEvents() {
        this.onFocusin.complete();
        this.onBlur.complete();
        this.onInput.complete();
        this.onKeydown.complete();
        this.onClick.complete();
    }
    /**
     * @param {?=} caretPos
     * @return {?}
     */
    focus(caretPos) {
        this.el.nativeElement.focus();
        this.el.nativeElement.setSelectionRange(caretPos, caretPos);
    }
    /**
     * @param {?} mention
     * @return {?}
     */
    insertMention(mention) {
        /** @type {?} */
        const value = this.el.nativeElement.value;
        /** @type {?} */
        const insertValue = mention.mention.trim() + ' ';
        /** @type {?} */
        const newValue = [
            value.slice(0, mention.startPos + 1),
            insertValue,
            value.slice(mention.endPos, value.length)
        ].join('');
        this.el.nativeElement.value = newValue;
        this.focus(mention.startPos + insertValue.length + 1);
        this.onChange(newValue);
        this.value = newValue;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    writeValue(value) {
        this.value = value;
        if (typeof value === 'string') {
            this.el.nativeElement.value = value;
        }
        else {
            this.el.nativeElement.value = '';
        }
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnChange(fn) {
        this.onChange = fn;
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnTouched(fn) {
        this.onTouched = fn;
    }
}
NzMentionTriggerDirective.decorators = [
    { type: Directive, args: [{
                selector: 'input[nzMentionTrigger], textarea[nzMentionTrigger]',
                providers: [NZ_MENTION_TRIGGER_ACCESSOR],
                host: {
                    autocomplete: 'off',
                    '(focusin)': 'onFocusin.emit()',
                    '(blur)': 'onBlur.emit()',
                    '(input)': 'onInput.emit($event)',
                    '(keydown)': 'onKeydown.emit($event)',
                    '(click)': 'onClick.emit($event)'
                }
            },] }
];
/** @nocollapse */
NzMentionTriggerDirective.ctorParameters = () => [
    { type: ElementRef }
];
if (false) {
    /** @type {?} */
    NzMentionTriggerDirective.prototype.onChange;
    /** @type {?} */
    NzMentionTriggerDirective.prototype.onTouched;
    /** @type {?} */
    NzMentionTriggerDirective.prototype.onFocusin;
    /** @type {?} */
    NzMentionTriggerDirective.prototype.onBlur;
    /** @type {?} */
    NzMentionTriggerDirective.prototype.onInput;
    /** @type {?} */
    NzMentionTriggerDirective.prototype.onKeydown;
    /** @type {?} */
    NzMentionTriggerDirective.prototype.onClick;
    /** @type {?} */
    NzMentionTriggerDirective.prototype.value;
    /** @type {?} */
    NzMentionTriggerDirective.prototype.el;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbWVudGlvbi10cmlnZ2VyLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbIm1lbnRpb24vbnotbWVudGlvbi10cmlnZ2VyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsVUFBVSxFQUFFLFNBQVMsRUFBRSxVQUFVLEVBQUUsWUFBWSxFQUErQixNQUFNLGVBQWUsQ0FBQztBQUM3RyxPQUFPLEVBQXdCLGlCQUFpQixFQUFFLE1BQU0sZ0JBQWdCLENBQUM7O0FBSXpFLE1BQU0sT0FBTywyQkFBMkIsR0FBcUI7SUFDM0QsT0FBTyxFQUFFLGlCQUFpQjtJQUMxQixXQUFXLEVBQUUsVUFBVTs7O0lBQUMsR0FBRyxFQUFFLENBQUMseUJBQXlCLEVBQUM7SUFDeEQsS0FBSyxFQUFFLElBQUk7Q0FDWjtBQWNELE1BQU0sT0FBTyx5QkFBeUI7Ozs7SUFXcEMsWUFBbUIsRUFBYztRQUFkLE9BQUUsR0FBRixFQUFFLENBQVk7UUFQeEIsY0FBUyxHQUF1QixJQUFJLFlBQVksRUFBRSxDQUFDO1FBQ25ELFdBQU0sR0FBdUIsSUFBSSxZQUFZLEVBQUUsQ0FBQztRQUNoRCxZQUFPLEdBQWdDLElBQUksWUFBWSxFQUFFLENBQUM7UUFDMUQsY0FBUyxHQUFnQyxJQUFJLFlBQVksRUFBRSxDQUFDO1FBQzVELFlBQU8sR0FBNkIsSUFBSSxZQUFZLEVBQUUsQ0FBQztJQUc1QixDQUFDOzs7O0lBRXJDLFdBQVc7UUFDVCxJQUFJLENBQUMsY0FBYyxFQUFFLENBQUM7SUFDeEIsQ0FBQzs7OztJQUVELGNBQWM7UUFDWixJQUFJLENBQUMsU0FBUyxDQUFDLFFBQVEsRUFBRSxDQUFDO1FBQzFCLElBQUksQ0FBQyxNQUFNLENBQUMsUUFBUSxFQUFFLENBQUM7UUFDdkIsSUFBSSxDQUFDLE9BQU8sQ0FBQyxRQUFRLEVBQUUsQ0FBQztRQUN4QixJQUFJLENBQUMsU0FBUyxDQUFDLFFBQVEsRUFBRSxDQUFDO1FBQzFCLElBQUksQ0FBQyxPQUFPLENBQUMsUUFBUSxFQUFFLENBQUM7SUFDMUIsQ0FBQzs7Ozs7SUFFRCxLQUFLLENBQUMsUUFBaUI7UUFDckIsSUFBSSxDQUFDLEVBQUUsQ0FBQyxhQUFhLENBQUMsS0FBSyxFQUFFLENBQUM7UUFDOUIsSUFBSSxDQUFDLEVBQUUsQ0FBQyxhQUFhLENBQUMsaUJBQWlCLENBQUMsUUFBUSxFQUFFLFFBQVEsQ0FBQyxDQUFDO0lBQzlELENBQUM7Ozs7O0lBRUQsYUFBYSxDQUFDLE9BQWdCOztjQUN0QixLQUFLLEdBQVcsSUFBSSxDQUFDLEVBQUUsQ0FBQyxhQUFhLENBQUMsS0FBSzs7Y0FDM0MsV0FBVyxHQUFHLE9BQU8sQ0FBQyxPQUFPLENBQUMsSUFBSSxFQUFFLEdBQUcsR0FBRzs7Y0FDMUMsUUFBUSxHQUFHO1lBQ2YsS0FBSyxDQUFDLEtBQUssQ0FBQyxDQUFDLEVBQUUsT0FBTyxDQUFDLFFBQVEsR0FBRyxDQUFDLENBQUM7WUFDcEMsV0FBVztZQUNYLEtBQUssQ0FBQyxLQUFLLENBQUMsT0FBTyxDQUFDLE1BQU0sRUFBRSxLQUFLLENBQUMsTUFBTSxDQUFDO1NBQzFDLENBQUMsSUFBSSxDQUFDLEVBQUUsQ0FBQztRQUNWLElBQUksQ0FBQyxFQUFFLENBQUMsYUFBYSxDQUFDLEtBQUssR0FBRyxRQUFRLENBQUM7UUFDdkMsSUFBSSxDQUFDLEtBQUssQ0FBQyxPQUFPLENBQUMsUUFBUSxHQUFHLFdBQVcsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxDQUFDLENBQUM7UUFDdEQsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsQ0FBQztRQUN4QixJQUFJLENBQUMsS0FBSyxHQUFHLFFBQVEsQ0FBQztJQUN4QixDQUFDOzs7OztJQUVELFVBQVUsQ0FBQyxLQUFhO1FBQ3RCLElBQUksQ0FBQyxLQUFLLEdBQUcsS0FBSyxDQUFDO1FBQ25CLElBQUksT0FBTyxLQUFLLEtBQUssUUFBUSxFQUFFO1lBQzdCLElBQUksQ0FBQyxFQUFFLENBQUMsYUFBYSxDQUFDLEtBQUssR0FBRyxLQUFLLENBQUM7U0FDckM7YUFBTTtZQUNMLElBQUksQ0FBQyxFQUFFLENBQUMsYUFBYSxDQUFDLEtBQUssR0FBRyxFQUFFLENBQUM7U0FDbEM7SUFDSCxDQUFDOzs7OztJQUVELGdCQUFnQixDQUFDLEVBQTJCO1FBQzFDLElBQUksQ0FBQyxRQUFRLEdBQUcsRUFBRSxDQUFDO0lBQ3JCLENBQUM7Ozs7O0lBRUQsaUJBQWlCLENBQUMsRUFBYztRQUM5QixJQUFJLENBQUMsU0FBUyxHQUFHLEVBQUUsQ0FBQztJQUN0QixDQUFDOzs7WUF2RUYsU0FBUyxTQUFDO2dCQUNULFFBQVEsRUFBRSxxREFBcUQ7Z0JBQy9ELFNBQVMsRUFBRSxDQUFDLDJCQUEyQixDQUFDO2dCQUN4QyxJQUFJLEVBQUU7b0JBQ0osWUFBWSxFQUFFLEtBQUs7b0JBQ25CLFdBQVcsRUFBRSxrQkFBa0I7b0JBQy9CLFFBQVEsRUFBRSxlQUFlO29CQUN6QixTQUFTLEVBQUUsc0JBQXNCO29CQUNqQyxXQUFXLEVBQUUsd0JBQXdCO29CQUNyQyxTQUFTLEVBQUUsc0JBQXNCO2lCQUNsQzthQUNGOzs7O1lBdEIrQixVQUFVOzs7O0lBd0J4Qyw2Q0FBa0M7O0lBQ2xDLDhDQUFzQjs7SUFFdEIsOENBQTREOztJQUM1RCwyQ0FBeUQ7O0lBQ3pELDRDQUFtRTs7SUFDbkUsOENBQXFFOztJQUNyRSw0Q0FBZ0U7O0lBQ2hFLDBDQUFjOztJQUVGLHVDQUFxQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IGZvcndhcmRSZWYsIERpcmVjdGl2ZSwgRWxlbWVudFJlZiwgRXZlbnRFbWl0dGVyLCBFeGlzdGluZ1Byb3ZpZGVyLCBPbkRlc3Ryb3kgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IENvbnRyb2xWYWx1ZUFjY2Vzc29yLCBOR19WQUxVRV9BQ0NFU1NPUiB9IGZyb20gJ0Bhbmd1bGFyL2Zvcm1zJztcblxuaW1wb3J0IHsgTWVudGlvbiB9IGZyb20gJy4vbnotbWVudGlvbi5jb21wb25lbnQnO1xuXG5leHBvcnQgY29uc3QgTlpfTUVOVElPTl9UUklHR0VSX0FDQ0VTU09SOiBFeGlzdGluZ1Byb3ZpZGVyID0ge1xuICBwcm92aWRlOiBOR19WQUxVRV9BQ0NFU1NPUixcbiAgdXNlRXhpc3Rpbmc6IGZvcndhcmRSZWYoKCkgPT4gTnpNZW50aW9uVHJpZ2dlckRpcmVjdGl2ZSksXG4gIG11bHRpOiB0cnVlXG59O1xuXG5ARGlyZWN0aXZlKHtcbiAgc2VsZWN0b3I6ICdpbnB1dFtuek1lbnRpb25UcmlnZ2VyXSwgdGV4dGFyZWFbbnpNZW50aW9uVHJpZ2dlcl0nLFxuICBwcm92aWRlcnM6IFtOWl9NRU5USU9OX1RSSUdHRVJfQUNDRVNTT1JdLFxuICBob3N0OiB7XG4gICAgYXV0b2NvbXBsZXRlOiAnb2ZmJyxcbiAgICAnKGZvY3VzaW4pJzogJ29uRm9jdXNpbi5lbWl0KCknLFxuICAgICcoYmx1ciknOiAnb25CbHVyLmVtaXQoKScsXG4gICAgJyhpbnB1dCknOiAnb25JbnB1dC5lbWl0KCRldmVudCknLFxuICAgICcoa2V5ZG93biknOiAnb25LZXlkb3duLmVtaXQoJGV2ZW50KScsXG4gICAgJyhjbGljayknOiAnb25DbGljay5lbWl0KCRldmVudCknXG4gIH1cbn0pXG5leHBvcnQgY2xhc3MgTnpNZW50aW9uVHJpZ2dlckRpcmVjdGl2ZSBpbXBsZW1lbnRzIENvbnRyb2xWYWx1ZUFjY2Vzc29yLCBPbkRlc3Ryb3kge1xuICBvbkNoYW5nZTogKHZhbHVlOiBzdHJpbmcpID0+IHZvaWQ7XG4gIG9uVG91Y2hlZDogKCkgPT4gdm9pZDtcblxuICByZWFkb25seSBvbkZvY3VzaW46IEV2ZW50RW1pdHRlcjx2b2lkPiA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcbiAgcmVhZG9ubHkgb25CbHVyOiBFdmVudEVtaXR0ZXI8dm9pZD4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIHJlYWRvbmx5IG9uSW5wdXQ6IEV2ZW50RW1pdHRlcjxLZXlib2FyZEV2ZW50PiA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcbiAgcmVhZG9ubHkgb25LZXlkb3duOiBFdmVudEVtaXR0ZXI8S2V5Ym9hcmRFdmVudD4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIHJlYWRvbmx5IG9uQ2xpY2s6IEV2ZW50RW1pdHRlcjxNb3VzZUV2ZW50PiA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcbiAgdmFsdWU6IHN0cmluZztcblxuICBjb25zdHJ1Y3RvcihwdWJsaWMgZWw6IEVsZW1lbnRSZWYpIHt9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5jb21wbGV0ZUV2ZW50cygpO1xuICB9XG5cbiAgY29tcGxldGVFdmVudHMoKTogdm9pZCB7XG4gICAgdGhpcy5vbkZvY3VzaW4uY29tcGxldGUoKTtcbiAgICB0aGlzLm9uQmx1ci5jb21wbGV0ZSgpO1xuICAgIHRoaXMub25JbnB1dC5jb21wbGV0ZSgpO1xuICAgIHRoaXMub25LZXlkb3duLmNvbXBsZXRlKCk7XG4gICAgdGhpcy5vbkNsaWNrLmNvbXBsZXRlKCk7XG4gIH1cblxuICBmb2N1cyhjYXJldFBvcz86IG51bWJlcik6IHZvaWQge1xuICAgIHRoaXMuZWwubmF0aXZlRWxlbWVudC5mb2N1cygpO1xuICAgIHRoaXMuZWwubmF0aXZlRWxlbWVudC5zZXRTZWxlY3Rpb25SYW5nZShjYXJldFBvcywgY2FyZXRQb3MpO1xuICB9XG5cbiAgaW5zZXJ0TWVudGlvbihtZW50aW9uOiBNZW50aW9uKTogdm9pZCB7XG4gICAgY29uc3QgdmFsdWU6IHN0cmluZyA9IHRoaXMuZWwubmF0aXZlRWxlbWVudC52YWx1ZTtcbiAgICBjb25zdCBpbnNlcnRWYWx1ZSA9IG1lbnRpb24ubWVudGlvbi50cmltKCkgKyAnICc7XG4gICAgY29uc3QgbmV3VmFsdWUgPSBbXG4gICAgICB2YWx1ZS5zbGljZSgwLCBtZW50aW9uLnN0YXJ0UG9zICsgMSksXG4gICAgICBpbnNlcnRWYWx1ZSxcbiAgICAgIHZhbHVlLnNsaWNlKG1lbnRpb24uZW5kUG9zLCB2YWx1ZS5sZW5ndGgpXG4gICAgXS5qb2luKCcnKTtcbiAgICB0aGlzLmVsLm5hdGl2ZUVsZW1lbnQudmFsdWUgPSBuZXdWYWx1ZTtcbiAgICB0aGlzLmZvY3VzKG1lbnRpb24uc3RhcnRQb3MgKyBpbnNlcnRWYWx1ZS5sZW5ndGggKyAxKTtcbiAgICB0aGlzLm9uQ2hhbmdlKG5ld1ZhbHVlKTtcbiAgICB0aGlzLnZhbHVlID0gbmV3VmFsdWU7XG4gIH1cblxuICB3cml0ZVZhbHVlKHZhbHVlOiBzdHJpbmcpOiB2b2lkIHtcbiAgICB0aGlzLnZhbHVlID0gdmFsdWU7XG4gICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gJ3N0cmluZycpIHtcbiAgICAgIHRoaXMuZWwubmF0aXZlRWxlbWVudC52YWx1ZSA9IHZhbHVlO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmVsLm5hdGl2ZUVsZW1lbnQudmFsdWUgPSAnJztcbiAgICB9XG4gIH1cblxuICByZWdpc3Rlck9uQ2hhbmdlKGZuOiAodmFsdWU6IHN0cmluZykgPT4gdm9pZCk6IHZvaWQge1xuICAgIHRoaXMub25DaGFuZ2UgPSBmbjtcbiAgfVxuXG4gIHJlZ2lzdGVyT25Ub3VjaGVkKGZuOiAoKSA9PiB2b2lkKTogdm9pZCB7XG4gICAgdGhpcy5vblRvdWNoZWQgPSBmbjtcbiAgfVxufVxuIl19